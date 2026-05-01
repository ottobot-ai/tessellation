package io.constellationnetwork.security.mpt

import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.ext.cats.effect._
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.schema.swap.{AllowSpendOrdinal, AllowSpendReference}
import io.constellationnetwork.schema.tokenLock.{TokenLockOrdinal, TokenLockReference}
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** Reproduces the ord-9 cl1/dl1 writer-vs-replay divergence: when an accept() introduces the FIRST `lastTokenLockRefs` entry (sidecar key
  * absent in preSyncBytes), the writer-side `applyActiveAddressIndexDelta` and verify-side `replayActiveAddressIndexDelta` must produce
  * byte-equal MPT state. The pre-existing `MptCrossPathDeterminismSuite` only covers `balances`/`lastTxRefs`/`stateChanHashes` and missed
  * this code path entirely.
  */
object ActiveAddressIndexSuite extends MutableIOSuite {

  implicit val withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit =
    io.constellationnetwork.schema.mpt.WithdrawalTimeLimit.none

  implicit val stateProofSelector: GlobalStateProofSelector =
    GlobalStateProofSelector(SnapshotOrdinal(NonNegLong(Long.MaxValue)))

  type Res = (JsonSerializer[IO], Hasher[IO], SecurityProvider[IO])

  override def sharedResource: Resource[IO, Res] = for {
    sp <- SecurityProvider.forAsync[IO]
    implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
    h = Hasher.forJson[IO]
  } yield (j, h, sp)

  private val addr1 = Address("DAG0y4eLqhhXUafeE3mgBstezPTnr8L3tZjAtMWB")
  private val addr2 = Address("DAG07tqNLYW8jHU9emXcRTT3CfgCUoumwcLghopd")

  private val tlRef = TokenLockReference(TokenLockOrdinal(NonNegLong.unsafeFrom(1L)), Hash("a" * 64))
  private val asRef = AllowSpendReference(AllowSpendOrdinal(NonNegLong.unsafeFrom(1L)), Hash("b" * 64))

  test("first-time lastTokenLockRefs entry: writer == replay (empty preSyncBytes)") { res =>
    implicit val (j, h, _) = res
    val _ = (j, h)
    val acc = GlobalStateConverter.StateChangesAccumulator(
      lastTokenLockRefs = SortedMap(addr1 -> tlRef)
    )
    val ord = SnapshotOrdinal(NonNegLong.unsafeFrom(9L))

    for {
      wProducer <- InMemoryMerklePatriciaProducer.make[IO]()
      wStore <- MptStore.make[IO, GlobalStateKey](wProducer, GlobalStateKey.toHex[IO])
      _ <- wStore.syncFromStateChanges(acc, ord)
      wTrie <- wStore.build(ord)
      wRoot = wTrie.toOption.map(_.rootHash.value.show).getOrElse("none")
      wBytes <- wStore.allEntriesAsBytes

      replay <- GlobalStateConverter.toAccumulatorHexDelta[IO](acc, Map.empty[Hex, Array[Byte]])
      rProducer <- InMemoryMerklePatriciaProducer.make[IO]()
      _ <- rProducer.insertBytes(replay._1).void
      _ <- rProducer.remove(replay._2.toList)
      rTrie <- rProducer.buildForOrdinal(ord)
      rRoot = rTrie.toOption.map(_.rootHash.value.show).getOrElse("none")
      rBytes <- rProducer.entries
    } yield
      expect.all(
        wRoot == rRoot,
        wBytes.size == rBytes.size,
        wBytes.view.mapValues(_.toVector).toMap == rBytes.view.mapValues(_.toVector).toMap
      )
  }

  test("first-time lastAllowSpendRefs entries: writer == replay (empty preSyncBytes)") { res =>
    implicit val (j, h, _) = res
    val _ = (j, h)
    val acc = GlobalStateConverter.StateChangesAccumulator(
      lastAllowSpendRefs = SortedMap(addr1 -> asRef, addr2 -> asRef)
    )
    val ord = SnapshotOrdinal(NonNegLong.unsafeFrom(9L))

    for {
      wProducer <- InMemoryMerklePatriciaProducer.make[IO]()
      wStore <- MptStore.make[IO, GlobalStateKey](wProducer, GlobalStateKey.toHex[IO])
      _ <- wStore.syncFromStateChanges(acc, ord)
      wTrie <- wStore.build(ord)
      wRoot = wTrie.toOption.map(_.rootHash.value.show).getOrElse("none")
      wBytes <- wStore.allEntriesAsBytes

      replay <- GlobalStateConverter.toAccumulatorHexDelta[IO](acc, Map.empty[Hex, Array[Byte]])
      rProducer <- InMemoryMerklePatriciaProducer.make[IO]()
      _ <- rProducer.insertBytes(replay._1).void
      _ <- rProducer.remove(replay._2.toList)
      rTrie <- rProducer.buildForOrdinal(ord)
      rRoot = rTrie.toOption.map(_.rootHash.value.show).getOrElse("none")
      rBytes <- rProducer.entries
    } yield
      expect.all(
        wRoot == rRoot,
        wBytes.size == rBytes.size,
        wBytes.view.mapValues(_.toVector).toMap == rBytes.view.mapValues(_.toVector).toMap
      )
  }

  test("two-step: ord8 builds sidecar, ord9 extends it; writer == verify-replay against preSyncBytes") { res =>
    implicit val (j, h, _) = res
    val _ = (j, h)
    val acc8 = GlobalStateConverter.StateChangesAccumulator(
      lastTokenLockRefs = SortedMap(addr1 -> tlRef)
    )
    val acc9 = GlobalStateConverter.StateChangesAccumulator(
      lastTokenLockRefs = SortedMap(addr2 -> tlRef)
    )
    val ord9 = SnapshotOrdinal(NonNegLong.unsafeFrom(9L))

    for {
      wProducer <- InMemoryMerklePatriciaProducer.make[IO]()
      wStore <- MptStore.make[IO, GlobalStateKey](wProducer, GlobalStateKey.toHex[IO])
      _ <- wStore.syncFromStateChanges(acc8, SnapshotOrdinal(NonNegLong.unsafeFrom(8L)))
      preSyncBytes <- wStore.allEntriesAsBytes
      _ <- wStore.syncFromStateChanges(acc9, ord9)
      wTrie <- wStore.build(ord9)
      wRoot = wTrie.toOption.map(_.rootHash.value.show).getOrElse("none")
      wBytes <- wStore.allEntriesAsBytes

      replay <- GlobalStateConverter.toAccumulatorHexDelta[IO](acc9, preSyncBytes)
      upserts = replay._1
      removes = replay._2
      expectedBytes: Map[Hex, Array[Byte]] = (preSyncBytes -- removes) ++ upserts
      rTrie <- io.constellationnetwork.security.mpt.MerklePatriciaTrie
        .makeParallelFromBytes[IO](expectedBytes)
      rRoot = rTrie.rootHash.value.show
    } yield
      expect.all(
        wRoot == rRoot,
        wBytes.size == expectedBytes.size,
        wBytes.view.mapValues(_.toVector).toMap == expectedBytes.view.mapValues(_.toVector).toMap
      )
  }
}
