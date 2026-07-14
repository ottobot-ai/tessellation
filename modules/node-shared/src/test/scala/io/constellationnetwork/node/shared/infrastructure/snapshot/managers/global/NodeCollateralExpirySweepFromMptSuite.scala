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
import io.constellationnetwork.schema.nodeCollateral._
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.signature.{Signed, signature}
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs.nodeCollateralWithdrawalExpiryKeySetImmutableCodec

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** Spec assertions for the surviving MPT-backed NC-withdrawal expiry sweep.
  *
  * After the legacy in-memory `findExpiredWithdrawalsViaIndex(map)` was deleted, this suite asserts
  * `findExpiredWithdrawalsViaIndexFromMpt(prev, curr, limit)` directly. NC differs from AllowSpend/TokenLock:
  *
  *   1. Expiry predicate is `<=` (createdAt + WTL <= curr), so the sweep window is `(prev .. curr]` — `fromL = prev + 1`, `toL = curr` —
  *      inclusive upper, exclusive lower. 2. The expiry epoch is derived (`createdAt + withdrawalTimeLimit`) rather than stored in the
  *      record. The index bucket keys already encode the derived epoch, so the test seeds with a concrete `WithdrawalTimeLimit` so the
  *      writer materializes the expected bucket positions.
  */
object NodeCollateralExpirySweepFromMptSuite extends MutableIOSuite {

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

  private def sameBytes(left: Map[Hex, Array[Byte]], right: Map[Hex, Array[Byte]]): Boolean =
    left.keySet == right.keySet && left.forall { case (key, bytes) => right.get(key).exists(_.sameElements(bytes)) }

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

  private def toAddressHashPairs(
    m: SortedMap[Address, SortedSet[PendingNodeCollateralWithdrawal]]
  )(implicit h: Hasher[IO]): IO[Set[(Address, Hash)]] =
    m.toList.flatTraverse {
      case (addr, set) =>
        set.toList.traverse(w => w.event.toHashed.map(hashed => (addr, hashed.hash)))
    }.map(_.toSet)

  test("FromMpt sweep returns exactly the expiring records — mixed expired and not") { res =>
    implicit val (h, sp, js) = res
    for {
      kp1 <- KeyPairGenerator.makeKeyPair[IO]
      kp2 <- KeyPairGenerator.makeKeyPair[IO]
      addr1 = kp1.getPublic.toAddress
      addr2 = kp2.getPublic.toAddress
      peer1 = kp1.getPublic.toId.toPeerId
      peer2 = kp2.getPublic.toId.toPeerId

      // withdrawalTimeLimit = 100, curEpoch = 300
      // addr1 withdrawal createdAt=50  → expiry=150 <= 300 → expired
      // addr1 withdrawal createdAt=250 → expiry=350 > 300  → not expired
      // addr2 withdrawal createdAt=300 → expiry=400 > 300  → not expired
      wExpired = mkWithdrawal(addr1, peer1, EpochProgress(NonNegLong(50L)), "expired")
      wValidA = mkWithdrawal(addr1, peer1, EpochProgress(NonNegLong(250L)), "validA")
      wValidB = mkWithdrawal(addr2, peer2, EpochProgress(NonNegLong(300L)), "validB")

      lastActive = SortedMap(
        addr1 -> SortedSet(wExpired, wValidA),
        addr2 -> SortedSet(wValidB)
      )

      store <- mkSeededMptStore(lastActive)
      mgr = NodeCollateralStateManager.make[IO](GlobalStateReader.fromMptStore(store))

      currentEpoch = EpochProgress(NonNegLong(300L))
      prevEpoch = EpochProgress.MinValue

      index <- mgr.findExpiredWithdrawalsViaIndexFromMpt(prevEpoch, currentEpoch, withdrawalTimeLimit)
      indexPairs <- toAddressHashPairs(index)

      expectedHashed <- wExpired.event.toHashed
    } yield expect(indexPairs == Set((addr1, expectedHashed.hash)))
  }

  test("FromMpt sweep returns empty when nothing is expired") { res =>
    implicit val (h, sp, js) = res
    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      addr = kp.getPublic.toAddress
      peer = kp.getPublic.toId.toPeerId

      // createdAt=500, WTL=100 → expiry=600 > 500 → not expired yet
      wFresh = mkWithdrawal(addr, peer, EpochProgress(NonNegLong(500L)), "fresh")
      lastActive = SortedMap(addr -> SortedSet(wFresh))

      store <- mkSeededMptStore(lastActive)
      mgr = NodeCollateralStateManager.make[IO](GlobalStateReader.fromMptStore(store))

      currentEpoch = EpochProgress(NonNegLong(500L))
      prevEpoch = EpochProgress(NonNegLong(100L))

      index <- mgr.findExpiredWithdrawalsViaIndexFromMpt(prevEpoch, currentEpoch, withdrawalTimeLimit)
      indexPairs <- toAddressHashPairs(index)
    } yield expect(indexPairs.isEmpty)
  }

  test("FromMpt sweep captures expiry exactly at currentEpoch (inclusive upper bound)") { res =>
    implicit val (h, sp, js) = res
    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      addr = kp.getPublic.toAddress
      peer = kp.getPublic.toId.toPeerId

      // createdAt=200, WTL=100 → expiry=300; currentEpoch=300 → expired (`<=` predicate, inclusive upper)
      wBoundary = mkWithdrawal(addr, peer, EpochProgress(NonNegLong(200L)), "boundary")
      lastActive = SortedMap(addr -> SortedSet(wBoundary))

      store <- mkSeededMptStore(lastActive)
      mgr = NodeCollateralStateManager.make[IO](GlobalStateReader.fromMptStore(store))

      currentEpoch = EpochProgress(NonNegLong(300L))
      prevEpoch = EpochProgress.MinValue

      index <- mgr.findExpiredWithdrawalsViaIndexFromMpt(prevEpoch, currentEpoch, withdrawalTimeLimit)
      indexPairs <- toAddressHashPairs(index)

      expectedHashed <- wBoundary.event.toHashed
    } yield
      expect.all(
        indexPairs.size == 1,
        indexPairs == Set((addr, expectedHashed.hash))
      )
  }

  test("FromMpt sweep returns empty when previousEpochProgress >= epochProgress (same-epoch ordinals)") { res =>
    implicit val (h, sp, js) = res
    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      addr = kp.getPublic.toAddress
      peer = kp.getPublic.toId.toPeerId

      wExpired = mkWithdrawal(addr, peer, EpochProgress(NonNegLong(50L)), "wouldExpire")
      lastActive = SortedMap(addr -> SortedSet(wExpired))

      store <- mkSeededMptStore(lastActive)
      mgr = NodeCollateralStateManager.make[IO](GlobalStateReader.fromMptStore(store))

      sameEpoch = EpochProgress(NonNegLong(200L))
      index <- mgr.findExpiredWithdrawalsViaIndexFromMpt(sameEpoch, sameEpoch, withdrawalTimeLimit)
      indexPairs <- toAddressHashPairs(index)
    } yield expect(indexPairs.isEmpty)
  }

  test("end-to-end acceptNodeCollaterals: expired/unexpired tuples reflect the sweep") { res =>
    implicit val (h, sp, js) = res
    // Exercise the full method: the FromMpt sweep should produce one expired (addr1.wExpired) and the
    // unexpired map should retain the other two records (addr1.wValidA, addr2.wValidB).
    for {
      kp1 <- KeyPairGenerator.makeKeyPair[IO]
      kp2 <- KeyPairGenerator.makeKeyPair[IO]
      addr1 = kp1.getPublic.toAddress
      addr2 = kp2.getPublic.toAddress
      peer1 = kp1.getPublic.toId.toPeerId
      peer2 = kp2.getPublic.toId.toPeerId

      wExpired = mkWithdrawal(addr1, peer1, EpochProgress(NonNegLong(50L)), "expired")
      wValidA = mkWithdrawal(addr1, peer1, EpochProgress(NonNegLong(250L)), "validA")
      wValidB = mkWithdrawal(addr2, peer2, EpochProgress(NonNegLong(300L)), "validB")

      existingWithdrawals = SortedMap(
        addr1 -> SortedSet(wExpired, wValidA),
        addr2 -> SortedSet(wValidB)
      )

      info = GlobalSnapshotInfo.empty.copy(nodeCollateralWithdrawals = existingWithdrawals.some)

      store <- mkSeededMptStore(existingWithdrawals)
      mgr = NodeCollateralStateManager.make[IO](GlobalStateReader.fromMptStore(store))

      currentEpoch = EpochProgress(NonNegLong(300L))
      prevEpoch = EpochProgress.MinValue

      result <- mgr.acceptNodeCollaterals(info, currentEpoch, prevEpoch, withdrawalTimeLimit)
      (_, unexpired, expired) = result
    } yield
      expect.all(
        // addr1: one expired, one unexpired
        expired.get(addr1) == Some(SortedSet(wExpired)),
        unexpired.get(addr1) == Some(SortedSet(wValidA)),
        // addr2: one unexpired, no expiry
        unexpired.get(addr2) == Some(SortedSet(wValidB)),
        !expired.contains(addr2)
      )
  }

  test("FromMpt sweep returns all expiring records when window covers multiple epochs") { res =>
    implicit val (h, sp, js) = res
    for {
      kp1 <- KeyPairGenerator.makeKeyPair[IO]
      kp2 <- KeyPairGenerator.makeKeyPair[IO]
      addr1 = kp1.getPublic.toAddress
      addr2 = kp2.getPublic.toAddress
      peer1 = kp1.getPublic.toId.toPeerId
      peer2 = kp2.getPublic.toId.toPeerId

      // WTL=100. Expiry epochs: 201, 205, 210 (from createdAt 101, 105, 110).
      w201 = mkWithdrawal(addr1, peer1, EpochProgress(NonNegLong(101L)), "at201")
      w205 = mkWithdrawal(addr1, peer1, EpochProgress(NonNegLong(105L)), "at205")
      w210 = mkWithdrawal(addr2, peer2, EpochProgress(NonNegLong(110L)), "at210")

      lastActive = SortedMap(
        addr1 -> SortedSet(w201, w205),
        addr2 -> SortedSet(w210)
      )

      store <- mkSeededMptStore(lastActive)
      mgr = NodeCollateralStateManager.make[IO](GlobalStateReader.fromMptStore(store))

      // NC window is `(prev .. curr]`: fromL = 201, toL = 215. All three (201, 205, 210) fall inside.
      prevEpoch = EpochProgress(NonNegLong(200L))
      currentEpoch = EpochProgress(NonNegLong(215L))

      index <- mgr.findExpiredWithdrawalsViaIndexFromMpt(prevEpoch, currentEpoch, withdrawalTimeLimit)
      indexPairs <- toAddressHashPairs(index)

      hashedAt201 <- w201.event.toHashed
      hashedAt205 <- w205.event.toHashed
      hashedAt210 <- w210.event.toHashed
    } yield
      expect.all(
        indexPairs.size == 3,
        indexPairs == Set(
          (addr1, hashedAt201.hash),
          (addr1, hashedAt205.hash),
          (addr2, hashedAt210.hash)
        )
      )
  }

  test("FromMpt sweep excludes records whose expiry falls strictly before the window (exclusive lower)") { res =>
    implicit val (h, sp, js) = res
    // NC sweep window is `(prev .. curr]` — fromL = prev + 1. A record whose derived expiry equals `prev`
    // exactly is OUTSIDE the window and must NOT be returned.
    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      addr = kp.getPublic.toAddress
      peer = kp.getPublic.toId.toPeerId

      // createdAt=100, WTL=100 → expiry=200. With prevEpoch=200 and curEpoch=300, fromL=201, toL=300.
      // Bucket 200 is OUTSIDE the window — record should NOT be returned by the sweep.
      wAtPrev = mkWithdrawal(addr, peer, EpochProgress(NonNegLong(100L)), "atPrev")
      lastActive = SortedMap(addr -> SortedSet(wAtPrev))

      store <- mkSeededMptStore(lastActive)
      mgr = NodeCollateralStateManager.make[IO](GlobalStateReader.fromMptStore(store))

      prevEpoch = EpochProgress(NonNegLong(200L))
      currentEpoch = EpochProgress(NonNegLong(300L))

      index <- mgr.findExpiredWithdrawalsViaIndexFromMpt(prevEpoch, currentEpoch, withdrawalTimeLimit)
      indexPairs <- toAddressHashPairs(index)
    } yield expect(indexPairs.isEmpty)
  }

  test("FromMpt sweep rejects an absent withdrawal target at its exact physical key without mutation") { res =>
    implicit val (h, sp, js) = res

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = keyPair.getPublic.toAddress
      peer = keyPair.getPublic.toId.toPeerId
      withdrawal = mkWithdrawal(source, peer, EpochProgress(NonNegLong(50L)), "absent-target")
      store <- mkSeededMptStore(SortedMap(source -> SortedSet(withdrawal)))
      targetKey = GlobalStateKey.hypergraph(GlobalStateFieldId.NodeCollateralWithdrawals, source)
      targetHex <- GlobalStateKey.toHex[IO](targetKey)
      _ <- store.remove(targetKey)
      before <- store.allEntriesAsBytes
      result <- NodeCollateralStateManager
        .make[IO](GlobalStateReader.fromMptStore(store))
        .findExpiredWithdrawalsViaIndexFromMpt(
          EpochProgress(NonNegLong(149L)),
          EpochProgress(NonNegLong(150L)),
          withdrawalTimeLimit
        )
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

  test("FromMpt sweep rejects a valid withdrawal hash indexed at the wrong expiry epoch without mutation") { res =>
    implicit val (h, sp, js) = res

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = keyPair.getPublic.toAddress
      peer = keyPair.getPublic.toId.toPeerId
      // createdAt=50 and WTL=100 make epoch 150 the only valid expiry bucket.
      withdrawal = mkWithdrawal(source, peer, EpochProgress(NonNegLong(50L)), "wrong-expiry-epoch")
      store <- mkSeededMptStore(SortedMap(source -> SortedSet(withdrawal)))
      hashed <- withdrawal.event.toHashed
      forgedEpoch = EpochProgress(NonNegLong(149L))
      forgedBucketKey <- GlobalStateKey.expiryIndexKey[IO](
        SystemNamespaceLabel.ExpiryIndexNodeCollateralWithdrawals,
        forgedEpoch
      )
      _ <- store.insert[SortedSet[NodeCollateralWithdrawalExpiryKey]](
        forgedBucketKey,
        SortedSet(NodeCollateralWithdrawalExpiryKey(source, hashed.hash))
      )
      targetKey = GlobalStateKey.hypergraph(GlobalStateFieldId.NodeCollateralWithdrawals, source)
      targetHex <- GlobalStateKey.toHex[IO](targetKey)
      before <- store.allEntriesAsBytes
      result <- NodeCollateralStateManager
        .make[IO](GlobalStateReader.fromMptStore(store))
        .findExpiredWithdrawalsViaIndexFromMpt(
          EpochProgress(NonNegLong(148L)),
          forgedEpoch,
          withdrawalTimeLimit
        )
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
