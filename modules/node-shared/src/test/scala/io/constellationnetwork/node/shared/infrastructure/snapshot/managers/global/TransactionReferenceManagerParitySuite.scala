package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.data.NonEmptySet
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.ParentChildTree
import io.constellationnetwork.node.shared.domain.nakamoto.overlay._
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
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs.addressSetImmutableCodec
import io.constellationnetwork.serde.codecs.instances.TransactionReferenceCodec.{immutableCodec => txRefImmutable}

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** Phase B acceptance gate (#56.10).
  *
  * Asserts byte-parity between the two `TransactionReferenceManager` wirings:
  *   - against `MptStore[F, GlobalStateKey]` directly (legacy path), and
  *   - against `AcceptanceMpt.fromOverlay` over `MptOverlay.passthrough`.
  *
  * Both wirings receive the same input state and the same `acceptTransactionRefs` call; their results must be identical SortedMaps. If they
  * diverge, the algebra has changed read semantics and Phase C must NOT proceed until the divergence is understood.
  *
  * Stronger property than tests-pass: this is the cross-implementation parity gate. The test fixes a generated state, runs both wirings,
  * and asserts equality.
  */
object TransactionReferenceManagerParitySuite extends MutableIOSuite {
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

  /** Build an `MptStore` seeded with the supplied refs AND with the `ActiveAddressIndex` sidecar populated for `LastTxRefs`. The sidecar is
    * what `materializeLastTxRefsFromMpt` reads to recover the keyset, so the test must mirror what `accept()` writes via
    * `syncFromStateChanges` in production.
    */
  private def mkMptStoreWith(
    refs: SortedMap[Address, TransactionReference]
  )(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[MptStore[IO, GlobalStateKey]] =
    for {
      producer <- InMemoryMerklePatriciaProducer.make[IO]()
      store <- MptStore.make[IO, GlobalStateKey](producer, GlobalStateKey.toHex[IO])
      _ <- refs.toList.traverse_ {
        case (addr, ref) =>
          store.insert[TransactionReference](GlobalStateKey.hypergraph(GlobalStateFieldId.LastTxRefs, addr), ref)
      }
      // Populate the ActiveAddressIndex sidecar so materializeLastTxRefsFromMpt can recover the keyset.
      indexKey <- GlobalStateKey.activeAddressIndexKey[IO](GlobalStateFieldId.LastTxRefs)
      addrSet = SortedSet.from(refs.keys)
      _ <- store.insert[SortedSet[Address]](indexKey, addrSet)
    } yield store

  private def mkOverlayPassthrough(
    store: MptStore[IO, GlobalStateKey]
  ): IO[MptOverlay[IO, GlobalStateKey]] =
    ParentChildTree.make[IO].map(MptOverlay.passthrough[IO, GlobalStateKey](store, _))

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

  test("acceptTransactionRefs: MptStore-backed and AcceptanceMpt(passthrough)-backed produce identical results") { res =>
    implicit val (h, sp, js) = res
    for {
      kp1 <- KeyPairGenerator.makeKeyPair[IO]
      kp2 <- KeyPairGenerator.makeKeyPair[IO]
      kp3 <- KeyPairGenerator.makeKeyPair[IO]
      source = kp1.getPublic.toAddress
      destInMpt = kp2.getPublic.toAddress
      destNew = kp3.getPublic.toAddress

      preExisting = SortedMap[Address, TransactionReference](
        destInMpt -> TransactionReference(TransactionOrdinal(7L), testHash("existing"))
      )
      acceptedTxs = SortedSet(
        mkSignedTx(source, destInMpt),
        mkSignedTx(source, destNew)
      )

      // Wiring 1: against MptStore directly via fromMptStore adapter.
      storeA <- mkMptStoreWith(preExisting)
      managerA = TransactionReferenceManager.make[IO](GlobalStateReader.fromMptStore(storeA))
      deltasA <- managerA.acceptTransactionRefs(Map.empty, acceptedTxs)

      // Wiring 2: against AcceptanceMpt over passthrough overlay.
      storeB <- mkMptStoreWith(preExisting)
      overlayB <- mkOverlayPassthrough(storeB)
      handleB <- overlayB.checkout(BranchId(Hash("0" * 64)))
      mptB = AcceptanceMpt.fromOverlay[IO](overlayB, BranchId(Hash("0" * 64)), handleB)
      managerB = TransactionReferenceManager.make[IO](mptB)
      deltasB <- managerB.acceptTransactionRefs(Map.empty, acceptedTxs)
    } yield expect(deltasA == deltasB)
  }

  test("materializeLastTxRefsFromMpt: MptStore-backed and AcceptanceMpt(passthrough)-backed produce identical results") { res =>
    implicit val (h, sp, js) = res
    for {
      kp1 <- KeyPairGenerator.makeKeyPair[IO]
      kp2 <- KeyPairGenerator.makeKeyPair[IO]
      addr1 = kp1.getPublic.toAddress
      addr2 = kp2.getPublic.toAddress
      preExisting = SortedMap[Address, TransactionReference](
        addr1 -> TransactionReference(TransactionOrdinal(3L), testHash("a")),
        addr2 -> TransactionReference(TransactionOrdinal(11L), testHash("b"))
      )

      storeA <- mkMptStoreWith(preExisting)
      managerA = TransactionReferenceManager.make[IO](GlobalStateReader.fromMptStore(storeA))
      mapA <- managerA.materializeLastTxRefsFromMpt

      storeB <- mkMptStoreWith(preExisting)
      overlayB <- mkOverlayPassthrough(storeB)
      handleB <- overlayB.checkout(BranchId(Hash("0" * 64)))
      mptB = AcceptanceMpt.fromOverlay[IO](overlayB, BranchId(Hash("0" * 64)), handleB)
      managerB = TransactionReferenceManager.make[IO](mptB)
      mapB <- managerB.materializeLastTxRefsFromMpt
    } yield
      expect.all(
        mapA == preExisting,
        mapA == mapB
      )
  }

  test("materializeLastTxRefsFromMpt: empty MPT yields empty result on both wirings") { res =>
    implicit val (h, sp, js) = res
    for {
      storeA <- mkMptStoreWith(SortedMap.empty)
      managerA = TransactionReferenceManager.make[IO](GlobalStateReader.fromMptStore(storeA))
      mapA <- managerA.materializeLastTxRefsFromMpt

      storeB <- mkMptStoreWith(SortedMap.empty)
      overlayB <- mkOverlayPassthrough(storeB)
      handleB <- overlayB.checkout(BranchId(Hash("0" * 64)))
      mptB = AcceptanceMpt.fromOverlay[IO](overlayB, BranchId(Hash("0" * 64)), handleB)
      managerB = TransactionReferenceManager.make[IO](mptB)
      mapB <- managerB.materializeLastTxRefsFromMpt
    } yield
      expect.all(
        mapA.isEmpty,
        mapB.isEmpty
      )
  }
}
