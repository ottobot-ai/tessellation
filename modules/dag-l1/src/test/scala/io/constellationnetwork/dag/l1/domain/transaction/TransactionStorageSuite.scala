package io.constellationnetwork.dag.l1.domain.transaction

import java.security.KeyPair

import cats.data.NonEmptyList
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.SortedMap
import scala.concurrent.duration.DurationInt

import io.constellationnetwork.dag.l1.Main
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.nodeSharedKryoRegistrar
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.transaction._
import io.constellationnetwork.security._
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.transaction.TransactionGenerator

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong
import io.chrisdavenport.mapref.MapRef
import weaver.SimpleIOSuite

object TransactionStorageSuite extends SimpleIOSuite with TransactionGenerator {

  type TestResources = (
    TransactionStorage[IO],
    MapRef[IO, Address, Option[SortedMap[TransactionOrdinal, StoredTransaction]]],
    KeyPair,
    Address,
    KeyPair,
    Address,
    SecurityProvider[IO],
    Hasher[IO],
    Hasher[IO],
    KeyPair,
    Address
  )

  def testResources: Resource[IO, TestResources] =
    SecurityProvider.forAsync[IO].flatMap { implicit sp =>
      KryoSerializer.forAsync[IO](Main.kryoRegistrar ++ nodeSharedKryoRegistrar).flatMap { implicit kp =>
        for {
          implicit0(jhs: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
          implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
          transactions <- MapRef.ofConcurrentHashMap[IO, Address, SortedMap[TransactionOrdinal, StoredTransaction]]().asResource
          contextualTransactionValidator = ContextualTransactionValidator
            .make(TransactionLimitConfig(Balance(100000000L), 20.hours, TransactionFee(200000L), 43.seconds), None)
          transactionStorage = new TransactionStorage[IO](
            transactions,
            TransactionReference.empty,
            contextualTransactionValidator
          )
          key1 <- KeyPairGenerator.makeKeyPair.asResource
          address1 = key1.getPublic.toAddress
          key2 <- KeyPairGenerator.makeKeyPair.asResource
          address2 = key2.getPublic.toAddress
          key3 <- KeyPairGenerator.makeKeyPair.asResource
          address3 = key3.getPublic.toAddress
        } yield (transactionStorage, transactions, key1, address1, key2, address2, sp, h, Hasher.forKryo[IO], key3, address3)
      }
    }

  test("setting initial refs should fail if already set") {
    testResources.use {
      case (transactionStorage, transactionR, _, address1, _, _, _, _, _, _, _) =>
        for {
          _ <- transactionR(address1).set(SortedMap.empty[TransactionOrdinal, StoredTransaction].some)

          result <- transactionStorage
            .initByRefs(
              Map(address1 -> TransactionReference.empty),
              SnapshotOrdinal.MinValue
            )
            .attempt

        } yield expect(result.isLeft)
    }
  }

  test("setting initial refs should succeed if not already set") {
    testResources.use {
      case (transactionStorage, _, _, address1, _, _, _, _, _, _, _) =>
        val ordinal = SnapshotOrdinal.MinValue
        val ref = TransactionReference.empty
        for {
          _ <- transactionStorage
            .initByRefs(Map(address1 -> ref), ordinal)
          txs <- transactionStorage.getState

          expected = Map(address1 -> SortedMap(ref.ordinal -> MajorityTx(ref, ordinal)))
        } yield expect.eql(txs, expected)
    }
  }

  test("pull should take transactions in correct order minding the fees") {
    testResources.use {
      case (transactionStorage, _, key1, address1, key2, address2, sp, h, txHasher, key3, address3) =>
        implicit val securityProvider = sp
        implicit val hasher = h

        for {

          txsA <- generateTransactions(address1, key1, address2, 2, TransactionFee(3L), kHasher = txHasher, jHasher = h)

          txsB <- generateTransactions(address2, key2, address1, 2, TransactionFee(2L), kHasher = txHasher, jHasher = h)

          txsC <- generateTransactions(address3, key3, address2, 2, TransactionFee.zero, kHasher = txHasher, jHasher = h)

          txsA2 <- generateTransactions(
            address1,
            key1,
            address2,
            2,
            TransactionFee(1L),
            Some(TransactionReference(txsA.last.ordinal, txsA.last.hash)),
            kHasher = txHasher,
            jHasher = h
          )
          _ <- (txsC.toList ::: txsA.toList ::: txsA2.toList ::: txsB.toList).distinct
            .traverse(transactionStorage.tryPut(_, SnapshotOrdinal.MinValue, Balance(NonNegLong.MaxValue)))

          pulled <- transactionStorage.pull(6L)

        } yield expect.same(NonEmptyList.fromList(txsA.toList ::: txsB.toList ::: txsA2.toList), pulled)
    }
  }

  test("pull should take transactions in correct order minding the fees - large amount of transactions") {
    testResources.use {
      case (transactionStorage, _, key1, address1, key2, address2, sp, h, txHasher, key3, address3) =>
        implicit val securityProvider = sp
        implicit val hasher = h

        for {

          txsA <- generateTransactions(address1, key1, address2, 1, TransactionFee(10L), kHasher = txHasher, jHasher = h)

          txsB <- generateTransactions(
            address2,
            key2,
            address3,
            100,
            TransactionFee(1L),
            kHasher = txHasher,
            jHasher = h
          )

          txsBHigherFee <- generateTransactions(
            address2,
            key2,
            address3,
            1,
            TransactionFee(8L),
            Some(TransactionReference(txsB.last.ordinal, txsB.last.hash)),
            kHasher = txHasher,
            jHasher = h
          )

          _ <- (txsA.toList ::: txsB.toList ::: txsBHigherFee.toList).distinct
            .traverse(transactionStorage.tryPut(_, SnapshotOrdinal.MinValue, Balance(NonNegLong.MaxValue)))

          pulled <- transactionStorage.pull(50L)
        } yield expect.same(NonEmptyList.fromList(txsA.toList ::: txsBHigherFee.toList ::: txsB.take(48)), pulled)
    }
  }

  test("pull should be able to take both fee and feeless transactions in one pull") {
    testResources.use {
      case (transactionStorage, _, key1, address1, key2, address2, sp, h, txHasher, _, _) =>
        implicit val securityProvider = sp
        implicit val hasher = h

        for {
          txsA <- generateTransactions(address1, key1, address2, 2, TransactionFee(1L), kHasher = txHasher, jHasher = h)
          txsB <- generateTransactions(address2, key2, address1, 1, TransactionFee(0L), kHasher = txHasher, jHasher = h)
          _ <- (txsA.toList ::: txsB.toList).distinct
            .traverse(transactionStorage.tryPut(_, SnapshotOrdinal.MinValue, Balance(NonNegLong.MaxValue)))

          pulled <- transactionStorage.pull(10L)

        } yield expect.same(txsA.append(txsB.head).some, pulled)
    }
  }

  test("pull should limit transactions count to specified value") {
    testResources.use {
      case (transactionStorage, _, key1, address1, key2, address2, sp, h, txHasher, _, _) =>
        implicit val securityProvider = sp
        implicit val hasher = h

        for {
          txsA <- generateTransactions(address1, key1, address2, 2, TransactionFee(3L), kHasher = txHasher, jHasher = h)
          txsB <- generateTransactions(address2, key2, address1, 2, TransactionFee(2L), kHasher = txHasher, jHasher = h)
          txsA2 <- generateTransactions(
            address1,
            key1,
            address2,
            2,
            TransactionFee(1L),
            Some(TransactionReference(txsA.last.ordinal, txsA.last.hash)),
            kHasher = txHasher,
            jHasher = h
          )
          _ <- (txsA.toList ::: txsA2.toList ::: txsB.toList).distinct
            .traverse(transactionStorage.tryPut(_, SnapshotOrdinal.MinValue, Balance(NonNegLong.MaxValue)))

          pulled <- transactionStorage.pull(4L)

        } yield expect.same(NonEmptyList.fromList(txsA.toList ::: txsB.toList), pulled)
    }
  }

  // --- adoptForwardByRefs (cl1 routine forward-adopt; monotone — the 2026-07-08 L0-token double-spend strand fix) ---

  test("adoptForwardByRefs must NOT roll back a locally-ahead consistent chain (the cl1 adopt-rollback regression)") {
    testResources.use {
      case (transactionStorage, _, key1, address1, _, address2, sp, h, txHasher, _, _) =>
        implicit val securityProvider = sp
        implicit val hasher = h

        for {
          txs <- generateTransactions(address1, key1, address2, 5, kHasher = txHasher, jHasher = h)
          // Local mempool-accepted chain is at ordinal 5...
          _ <- txs.toList.traverse(transactionStorage.accept)
          // ...and the (lagging) finalized mirror adopt carries ordinal 2 — an ancestor of the local chain.
          _ <- transactionStorage.adoptForwardByRefs(
            Map(address1 -> TransactionReference.of(txs.toList(1))),
            SnapshotOrdinal.MinValue
          )
          lastProcessed <- transactionStorage.getLastProcessedTransaction(address1)
          state <- transactionStorage.getState
          stored = state(address1)
        } yield
          // The rolled-forward chain survives: last processed stays at ordinal 5, txs ≤ 2 compacted into the majority marker.
          expect.same(TransactionReference.of(txs.last), lastProcessed.ref) &&
            expect.same(
              SortedMap[TransactionOrdinal, StoredTransaction](
                txs.toList(1).ordinal -> MajorityTx(TransactionReference.of(txs.toList(1)), SnapshotOrdinal.MinValue),
                txs.toList(2).ordinal -> AcceptedTx(txs.toList(2)),
                txs.toList(3).ordinal -> AcceptedTx(txs.toList(3)),
                txs.toList(4).ordinal -> AcceptedTx(txs.toList(4))
              ),
              stored
            )
    }
  }

  test("adoptForwardByRefs adopts a finalized ref AHEAD of the local chain and re-attaches still-chaining local txs") {
    testResources.use {
      case (transactionStorage, transactionR, key1, address1, _, address2, sp, h, txHasher, _, _) =>
        implicit val securityProvider = sp
        implicit val hasher = h

        for {
          txs <- generateTransactions(address1, key1, address2, 4, kHasher = txHasher, jHasher = h)
          // Local state: majority base at ordinal 1, waiting txs at ordinals 3 and 4 (2 not yet seen locally).
          _ <- transactionR(address1).set(
            SortedMap[TransactionOrdinal, StoredTransaction](
              txs.head.ordinal -> MajorityTx(TransactionReference.of(txs.head), SnapshotOrdinal.MinValue),
              txs.toList(2).ordinal -> WaitingTx(txs.toList(2)),
              txs.toList(3).ordinal -> WaitingTx(txs.toList(3))
            ).some
          )
          // Finalized mirror advanced past us to ordinal 2.
          _ <- transactionStorage.adoptForwardByRefs(
            Map(address1 -> TransactionReference.of(txs.toList(1))),
            SnapshotOrdinal.MinValue
          )
          state <- transactionStorage.getState
          stored = state(address1)
        } yield
          // New majority base at ordinal 2; waiting 3 and 4 still chain onto it and survive; entry at 1 is compacted away.
          expect.same(
            SortedMap[TransactionOrdinal, StoredTransaction](
              txs.toList(1).ordinal -> MajorityTx(TransactionReference.of(txs.toList(1)), SnapshotOrdinal.MinValue),
              txs.toList(2).ordinal -> WaitingTx(txs.toList(2)),
              txs.toList(3).ordinal -> WaitingTx(txs.toList(3))
            ),
            stored
          )
    }
  }

  test("adoptForwardByRefs drops local txs that no longer chain onto the adopted finalized ref") {
    testResources.use {
      case (transactionStorage, transactionR, key1, address1, _, address2, sp, h, txHasher, key3, address3) =>
        implicit val securityProvider = sp
        implicit val hasher = h

        for {
          txs <- generateTransactions(address1, key1, address2, 3, kHasher = txHasher, jHasher = h)
          // A conflicting local chain: same source, same starting parent, DIFFERENT destination ⇒ different hashes.
          conflicting <- generateTransactions(address1, key1, address3, 3, kHasher = txHasher, jHasher = h)
          // Local state: majority base at ordinal 1 plus a waiting tx at ordinal 3 from the CONFLICTING branch.
          _ <- transactionR(address1).set(
            SortedMap[TransactionOrdinal, StoredTransaction](
              txs.head.ordinal -> MajorityTx(TransactionReference.of(txs.head), SnapshotOrdinal.MinValue),
              conflicting.toList(2).ordinal -> WaitingTx(conflicting.toList(2))
            ).some
          )
          // Finalized mirror advanced to ordinal 2 on the txs branch.
          _ <- transactionStorage.adoptForwardByRefs(
            Map(address1 -> TransactionReference.of(txs.toList(1))),
            SnapshotOrdinal.MinValue
          )
          state <- transactionStorage.getState
          stored = state(address1)
        } yield
          // conflicting(2) at ordinal 3 does NOT chain onto the adopted ref (parent = conflicting(1) ≠ txs(1)) ⇒ dropped.
          expect.same(
            SortedMap[TransactionOrdinal, StoredTransaction](
              txs.toList(1).ordinal -> MajorityTx(TransactionReference.of(txs.toList(1)), SnapshotOrdinal.MinValue)
            ),
            stored
          )
    }
  }

  test("adoptForwardByRefs resets destructively when the local chain DIVERGED at the finalized ordinal") {
    testResources.use {
      case (transactionStorage, transactionR, key1, address1, _, address2, sp, h, txHasher, key3, address3) =>
        implicit val securityProvider = sp
        implicit val hasher = h

        for {
          txs <- generateTransactions(address1, key1, address2, 3, kHasher = txHasher, jHasher = h)
          conflicting <- generateTransactions(address1, key1, address3, 3, kHasher = txHasher, jHasher = h)
          // Local chain accepted the CONFLICTING branch up to ordinal 3.
          _ <- transactionR(address1).set(
            SortedMap[TransactionOrdinal, StoredTransaction](
              conflicting.head.ordinal -> AcceptedTx(conflicting.head),
              conflicting.toList(1).ordinal -> AcceptedTx(conflicting.toList(1)),
              conflicting.toList(2).ordinal -> AcceptedTx(conflicting.toList(2))
            ).some
          )
          // Finalized mirror carries the OTHER branch's ordinal-2 tx — the local branch lost.
          _ <- transactionStorage.adoptForwardByRefs(
            Map(address1 -> TransactionReference.of(txs.toList(1))),
            SnapshotOrdinal.MinValue
          )
          state <- transactionStorage.getState
          stored = state(address1)
        } yield
          expect.same(
            SortedMap[TransactionOrdinal, StoredTransaction](
              txs.toList(1).ordinal -> MajorityTx(TransactionReference.of(txs.toList(1)), SnapshotOrdinal.MinValue)
            ),
            stored
          )
    }
  }

  test("adoptForwardByRefs is a no-op for a finalized ref behind the local majority base (append-only source)") {
    testResources.use {
      case (transactionStorage, transactionR, key1, address1, _, address2, sp, h, txHasher, _, _) =>
        implicit val securityProvider = sp
        implicit val hasher = h

        for {
          txs <- generateTransactions(address1, key1, address2, 5, kHasher = txHasher, jHasher = h)
          // Post-compaction local state: single majority marker at ordinal 5 (no entry at ordinal 2 anymore).
          _ <- transactionR(address1).set(
            SortedMap[TransactionOrdinal, StoredTransaction](
              txs.last.ordinal -> MajorityTx(TransactionReference.of(txs.last), SnapshotOrdinal.MinValue)
            ).some
          )
          _ <- transactionStorage.adoptForwardByRefs(
            Map(address1 -> TransactionReference.of(txs.toList(1))),
            SnapshotOrdinal.MinValue
          )
          state <- transactionStorage.getState
          stored = state(address1)
        } yield
          expect.same(
            SortedMap[TransactionOrdinal, StoredTransaction](
              txs.last.ordinal -> MajorityTx(TransactionReference.of(txs.last), SnapshotOrdinal.MinValue)
            ),
            stored
          )
    }
  }

  test("adoptForwardByRefs behaves like replaceByRefs on an empty address slot (cold bootstrap unchanged)") {
    testResources.use {
      case (transactionStorage, _, key1, address1, _, address2, sp, h, txHasher, _, _) =>
        implicit val securityProvider = sp
        implicit val hasher = h

        for {
          txs <- generateTransactions(address1, key1, address2, 1, kHasher = txHasher, jHasher = h)
          _ <- transactionStorage.adoptForwardByRefs(
            Map(address1 -> TransactionReference.of(txs.head)),
            SnapshotOrdinal.MinValue
          )
          state <- transactionStorage.getState
          stored = state(address1)
        } yield
          expect.same(
            SortedMap[TransactionOrdinal, StoredTransaction](
              txs.head.ordinal -> MajorityTx(TransactionReference.of(txs.head), SnapshotOrdinal.MinValue)
            ),
            stored
          )
    }
  }
}
