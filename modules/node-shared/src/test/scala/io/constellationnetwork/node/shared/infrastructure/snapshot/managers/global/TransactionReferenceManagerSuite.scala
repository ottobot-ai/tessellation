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
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey, MptStore}
import io.constellationnetwork.schema.transaction._
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.signature.{Signed, signature}
import io.constellationnetwork.serde.codecs.instances.TransactionReferenceCodec.{immutableCodec => txRefImmutable}

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

object TransactionReferenceManagerSuite extends MutableIOSuite {
  implicit val globalStateProofSelector: GlobalStateProofSelector = GlobalStateProofSelector(SnapshotOrdinal(NonNegLong(Long.MaxValue)))

  type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  private val testSignature = signature.Signature(Hex(""))
  private val testSignatureProof = signature.SignatureProof(Id(Hex("")), testSignature)
  private val testProofs = NonEmptySet.one(testSignatureProof)

  private def testHash(label: String): Hash =
    Hash(label.getBytes("UTF-8").map("%02x".format(_)).mkString.padTo(64, '0').take(64))

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp, j)

  private def mkMptStoreWith(
    refs: SortedMap[Address, TransactionReference]
  )(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[MptStore[IO, GlobalStateKey]] =
    for {
      mptProducer <- InMemoryMerklePatriciaProducer.make[IO]()
      mptStore <- MptStore.make[IO, GlobalStateKey](mptProducer, GlobalStateKey.toHex[IO])
      _ <- refs.toList.traverse_ {
        case (addr, ref) =>
          mptStore.insert[TransactionReference](GlobalStateKey.hypergraph(GlobalStateFieldId.LastTxRefs, addr), ref)
      }
    } yield mptStore

  private def mkEmptyMptStore(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[MptStore[IO, GlobalStateKey]] =
    for {
      mptProducer <- InMemoryMerklePatriciaProducer.make[IO]()
      mptStore <- MptStore.make[IO, GlobalStateKey](mptProducer, GlobalStateKey.toHex[IO])
    } yield mptStore

  private def mkSignedTx(source: Address, destination: Address): Signed[Transaction] =
    Signed(
      Transaction(
        source = source,
        destination = destination,
        amount = TransactionAmount(1L),
        fee = TransactionFee(0L),
        parent = TransactionReference(TransactionOrdinal(0L), testHash("parent")),
        salt = TransactionSalt(0L)
      ),
      testProofs
    )

  test("brand-new destination (not in MPT, not in contextUpdate) gets an empty-ref delta") { res =>
    implicit val (h, sp, js) = res
    for {
      kp1 <- KeyPairGenerator.makeKeyPair[IO]
      kp2 <- KeyPairGenerator.makeKeyPair[IO]
      source = kp1.getPublic.toAddress
      destNew = kp2.getPublic.toAddress

      acceptedTxs = SortedSet(mkSignedTx(source, destNew))

      mptStore <- mkEmptyMptStore
      manager = TransactionReferenceManager.make[IO](mptStore)
      deltas <- manager.acceptTransactionRefs(Map.empty, acceptedTxs)
    } yield expect(deltas == SortedMap(destNew -> TransactionReference.empty))
  }

  test("destination already in MPT gets no empty-ref delta") { res =>
    implicit val (h, sp, js) = res
    for {
      kp1 <- KeyPairGenerator.makeKeyPair[IO]
      kp2 <- KeyPairGenerator.makeKeyPair[IO]
      kp3 <- KeyPairGenerator.makeKeyPair[IO]
      source = kp1.getPublic.toAddress
      destKnown = kp2.getPublic.toAddress
      destNew = kp3.getPublic.toAddress

      preExisting = SortedMap[Address, TransactionReference](
        destKnown -> TransactionReference(TransactionOrdinal(5L), testHash("known"))
      )
      acceptedTxs = SortedSet(
        mkSignedTx(source, destKnown),
        mkSignedTx(source, destNew)
      )

      mptStore <- mkMptStoreWith(preExisting)
      manager = TransactionReferenceManager.make[IO](mptStore)
      deltas <- manager.acceptTransactionRefs(Map.empty, acceptedTxs)
    } yield expect(deltas == SortedMap(destNew -> TransactionReference.empty))
  }

  test("contextUpdate shadows MPT state (no empty-ref added for in-block delta)") { res =>
    implicit val (h, sp, js) = res
    for {
      kp1 <- KeyPairGenerator.makeKeyPair[IO]
      kp2 <- KeyPairGenerator.makeKeyPair[IO]
      source = kp1.getPublic.toAddress
      destInBlock = kp2.getPublic.toAddress

      inBlockRef = TransactionReference(TransactionOrdinal(1L), testHash("inblock"))
      contextUpdate = Map(destInBlock -> inBlockRef)
      acceptedTxs = SortedSet(mkSignedTx(source, destInBlock))

      mptStore <- mkEmptyMptStore
      manager = TransactionReferenceManager.make[IO](mptStore)
      deltas <- manager.acceptTransactionRefs(contextUpdate, acceptedTxs)
    } yield expect(deltas == SortedMap(destInBlock -> inBlockRef))
  }

  test("multiple txs to the same new destination yield one empty-ref entry") { res =>
    implicit val (h, sp, js) = res
    for {
      kp1 <- KeyPairGenerator.makeKeyPair[IO]
      kp2 <- KeyPairGenerator.makeKeyPair[IO]
      source = kp1.getPublic.toAddress
      dest = kp2.getPublic.toAddress

      acceptedTxs = SortedSet(
        mkSignedTx(source, dest),
        mkSignedTx(source, dest)
      )

      mptStore <- mkEmptyMptStore
      manager = TransactionReferenceManager.make[IO](mptStore)
      deltas <- manager.acceptTransactionRefs(Map.empty, acceptedTxs)
    } yield expect(deltas == SortedMap(dest -> TransactionReference.empty))
  }

  test("empty inputs yield empty deltas") { res =>
    implicit val (h, sp, js) = res
    for {
      mptStore <- mkEmptyMptStore
      manager = TransactionReferenceManager.make[IO](mptStore)
      deltas <- manager.acceptTransactionRefs(Map.empty, SortedSet.empty)
    } yield expect(deltas.isEmpty)
  }
}
