package io.constellationnetwork.schema.mpt

import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.generators.addressGen
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.MerklePatriciaTrie
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs._

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** Regression contract for the complete consensus global MPT root.
  *
  * `SystemNamespace` entries are economically consumed state, not disposable acceleration sidecars. Their exact bytes must therefore be
  * retained in `consensusRootEntries` and committed by the aggregate `mptRoot`. They do not populate unrelated per-field proof slots.
  * `MgGlobalSnapshotSyncView` remains the one explicitly excluded observation field.
  */
object GlobalMptRootCompletenessSuite extends MutableIOSuite {

  implicit val globalStateProofSelector: GlobalStateProofSelector =
    GlobalStateProofSelector(SnapshotOrdinal(NonNegLong(Long.MaxValue)))
  implicit val withdrawalTimeLimit: WithdrawalTimeLimit = WithdrawalTimeLimit.none

  type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp, j)

  private def bal(n: Long): Balance = Balance(NonNegLong.unsafeFrom(n))

  private def sampleGsi: GlobalSnapshotInfo = {
    val addrs = (0 until 6).map(_ => addressGen.sample.get).distinct.toList
    GlobalSnapshotInfo.empty.copy(
      balances = SortedMap.from(addrs.zipWithIndex.map { case (a, i) => a -> bal((i + 1) * 1000L) }),
      lastTxRefs = SortedMap.empty
    )
  }

  private def testHash(label: String): Hash =
    Hash(label.getBytes("UTF-8").map("%02x".format(_)).mkString.padTo(64, '0').take(64))

  private def freshAddress(excluding: Set[Address]): Address =
    Iterator.continually(addressGen.sample.get).find(a => !excluding.contains(a)).get

  private def stateBytes(
    gsi: GlobalSnapshotInfo
  )(implicit h: Hasher[IO], j: JsonSerializer[IO]): IO[Map[Hex, Array[Byte]]] = {
    import GlobalStateConverter.syntax._

    gsi.allStateEntriesAsBytes.flatMap { typed =>
      typed.toList.traverse { case (k, v) => GlobalStateKey.toHex[IO](k).map(_ -> v) }.map(_.toMap)
    }
  }

  private def activeAddressIndexEntry(
    fieldId: GlobalStateFieldId,
    addresses: SortedSet[Address]
  )(implicit h: Hasher[IO]): IO[(Hex, Array[Byte])] =
    GlobalStateKey.activeAddressIndexKey[IO](fieldId).flatMap(GlobalStateKey.toHex[IO]).map { hex =>
      hex -> ImmutableCodec[SortedSet[Address]].immutableBytes(addresses).toArray
    }

  private def expiryEntries(
    address: Address
  )(implicit h: Hasher[IO]): IO[List[(SystemNamespaceLabel, Hex, Array[Byte])]] = {
    val epoch = EpochProgress(NonNegLong(100L))
    val values = List(
      SystemNamespaceLabel.ExpiryIndexAllowSpends ->
        ImmutableCodec[SortedSet[AllowSpendExpiryKey]]
          .immutableBytes(SortedSet(AllowSpendExpiryKey(None, address, testHash("allow-spend"))))
          .toArray,
      SystemNamespaceLabel.ExpiryIndexTokenLocks ->
        ImmutableCodec[SortedSet[TokenLockExpiryKey]]
          .immutableBytes(SortedSet(TokenLockExpiryKey(address, testHash("token-lock"))))
          .toArray,
      SystemNamespaceLabel.ExpiryIndexNodeCollateralWithdrawals ->
        ImmutableCodec[SortedSet[NodeCollateralWithdrawalExpiryKey]]
          .immutableBytes(SortedSet(NodeCollateralWithdrawalExpiryKey(address, testHash("collateral"))))
          .toArray
    )

    values.traverse {
      case (label, bytes) =>
        GlobalStateKey.expiryIndexKey[IO](label, epoch).flatMap(GlobalStateKey.toHex[IO]).map(hex => (label, hex, bytes))
    }
  }

  private def completeRoot(
    entries: Map[Hex, Array[Byte]]
  )(implicit h: Hasher[IO], j: JsonSerializer[IO]): IO[Hash] =
    MerklePatriciaTrie.makeParallelFromBytes[IO](entries).map(_.rootHash.value)

  private def sameBytes(left: Map[Hex, Array[Byte]], right: Map[Hex, Array[Byte]]): Boolean =
    left.keySet == right.keySet && left.forall {
      case (key, bytes) => right.get(key).exists(_.sameElements(bytes))
    }

  private def sameExposedFieldRoots(left: GlobalSnapshotStateProof, right: GlobalSnapshotStateProof): Boolean =
    left.copy(mptRoot = None) == right.copy(mptRoot = None)

  test("adding a valid ActiveAddressIndex entry changes only the aggregate consensus mptRoot") { res =>
    implicit val (h, _, j) = res
    val gsi = sampleGsi

    for {
      base <- stateBytes(gsi)
      entry <- activeAddressIndexEntry(GlobalStateFieldId.Balances, gsi.balances.keySet.to(SortedSet))
      (indexHex, indexBytes) = entry
      withoutIndex = base - indexHex
      withIndex = base.updated(indexHex, indexBytes)
      baseProof <- GlobalSnapshotInfo.mptStateProofFromBytes[IO](withoutIndex)
      indexedProof <- GlobalSnapshotInfo.mptStateProofFromBytes[IO](withIndex)
    } yield
      expect.all(
        GlobalStateKey.isSystemNamespaceHex(indexHex),
        GlobalStateKey.consensusRootEntries(withIndex).contains(indexHex),
        baseProof.mptRoot.isDefined,
        indexedProof.mptRoot.isDefined,
        baseProof.mptRoot != indexedProof.mptRoot,
        sameExposedFieldRoots(baseProof, indexedProof)
      )
  }

  test("changing ActiveAddressIndex contents changes only the aggregate consensus mptRoot") { res =>
    implicit val (h, _, j) = res
    val gsi = sampleGsi
    val current = gsi.balances.keySet.to(SortedSet)
    val stale = freshAddress(current.toSet)

    for {
      base <- stateBytes(gsi)
      entryA <- activeAddressIndexEntry(GlobalStateFieldId.Balances, current)
      entryB <- activeAddressIndexEntry(GlobalStateFieldId.Balances, current + stale)
      (indexHexA, indexBytesA) = entryA
      (indexHexB, indexBytesB) = entryB
      proofA <- GlobalSnapshotInfo.mptStateProofFromBytes[IO](base.updated(indexHexA, indexBytesA))
      proofB <- GlobalSnapshotInfo.mptStateProofFromBytes[IO](base.updated(indexHexB, indexBytesB))
    } yield
      expect.all(
        indexHexA == indexHexB,
        !indexBytesA.sameElements(indexBytesB),
        proofA.mptRoot != proofB.mptRoot,
        sameExposedFieldRoots(proofA, proofB)
      )
  }

  test("every expiry SystemNamespace label is retained and changes only the aggregate consensus mptRoot") { res =>
    implicit val (h, _, j) = res
    val gsi = sampleGsi

    for {
      base <- stateBytes(gsi)
      baseProof <- GlobalSnapshotInfo.mptStateProofFromBytes[IO](base)
      entries <- expiryEntries(gsi.balances.keySet.head)
      observations <- entries.traverse {
        case (label, hex, bytes) =>
          val withExpiry = base.updated(hex, bytes)
          GlobalSnapshotInfo.mptStateProofFromBytes[IO](withExpiry).map { proof =>
            (
              label,
              GlobalStateKey.isSystemNamespaceHex(hex),
              GlobalStateKey.consensusRootEntries(withExpiry).contains(hex),
              baseProof.mptRoot != proof.mptRoot,
              sameExposedFieldRoots(baseProof, proof)
            )
          }
      }
      expectedLabels = List(
        SystemNamespaceLabel.ExpiryIndexAllowSpends,
        SystemNamespaceLabel.ExpiryIndexTokenLocks,
        SystemNamespaceLabel.ExpiryIndexNodeCollateralWithdrawals
      )
    } yield
      expect.same(observations.map(_._1), expectedLabels) &&
        expect(observations.forall(_._2)) &&
        expect(observations.forall(_._3)) &&
        expect(observations.forall(_._4)) &&
        expect(observations.forall(_._5))
  }

  // Temporary ECO-F32 containment, not a claim that the current replay path is sound. ROOT-010 first binds the exact replay witness,
  // then removes this field from every GL0 write/load path while ML0 retains it in CurrencySnapshotInfo.
  test("MgGlobalSnapshotSyncView remains excluded from consensusRootEntries and the aggregate root") { res =>
    implicit val (h, _, j) = res
    val gsi = sampleGsi
    val metagraphId = gsi.balances.keySet.head

    for {
      base <- stateBytes(gsi)
      syncKey <- GlobalStateKey.metagraphEntryHashed[IO](metagraphId, GlobalStateFieldId.MgGlobalSnapshotSyncView, "peer-1")
      syncHex <- GlobalStateKey.toHex[IO](syncKey)
      withSync = base.updated(syncHex, Array[Byte](1, 2, 3))
      baseProof <- GlobalSnapshotInfo.mptStateProofFromBytes[IO](base)
      syncProof <- GlobalSnapshotInfo.mptStateProofFromBytes[IO](withSync)
      consensus = GlobalStateKey.consensusRootEntries(withSync)
    } yield
      expect.all(
        withSync.contains(syncHex),
        GlobalStateKey.fieldIdFromHex(syncHex).contains(GlobalStateFieldId.MgGlobalSnapshotSyncView),
        !consensus.contains(syncHex),
        consensus.keySet == base.keySet,
        baseProof == syncProof
      )
  }

  test("loadBytes preserves exact complete-root bytes and the complete consensus root") { res =>
    implicit val (h, _, j) = res
    val gsi = sampleGsi
    val ordinal = SnapshotOrdinal(NonNegLong(1L))

    for {
      base <- stateBytes(gsi)
      active <- activeAddressIndexEntry(GlobalStateFieldId.Balances, gsi.balances.keySet.to(SortedSet))
      expiry <- expiryEntries(gsi.balances.keySet.head)
      signedBytes = expiry.foldLeft(base.updated(active._1, active._2)) { case (entries, (_, hex, bytes)) =>
        entries.updated(hex, bytes)
      }
      signedCompleteRoot <- completeRoot(signedBytes)
      producer <- InMemoryMerklePatriciaProducer.make[IO]()
      store <- MptStore.make[IO, GlobalStateKey](producer, GlobalStateKey.toHex[IO])
      _ <- store.loadBytes(signedBytes, ordinal)
      stored <- store.allEntriesAsBytes
      storedCompleteRoot <- completeRoot(stored)
      storedConsensusRoot <- completeRoot(GlobalStateKey.consensusRootEntries(stored))
    } yield
      expect.all(
        sameBytes(stored, signedBytes),
        storedCompleteRoot == signedCompleteRoot,
        storedConsensusRoot == signedCompleteRoot
      )
  }
}
