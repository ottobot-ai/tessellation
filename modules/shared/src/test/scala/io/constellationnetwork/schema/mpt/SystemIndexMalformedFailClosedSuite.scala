package io.constellationnetwork.schema.mpt

import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.GlobalStateConverter.StateChangesAccumulator
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.{GlobalStateProofSelector, SnapshotOrdinal}
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs._

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

object SystemIndexMalformedFailClosedSuite extends MutableIOSuite {

  implicit val stateProofSelector: GlobalStateProofSelector =
    GlobalStateProofSelector(SnapshotOrdinal(NonNegLong(Long.MaxValue)))

  type Res = (Hasher[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      implicit0(json: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      hasher = Hasher.forJson[IO]
    } yield (hasher, json)

  private val garbage = Array[Byte](0x7f)
  private val addressA = Address.fromBytes("malformed-system-index-a".getBytes("UTF-8"))
  private val addressB = Address.fromBytes("malformed-system-index-b".getBytes("UTF-8"))
  private def freshStore(
    implicit hasher: Hasher[IO],
    json: JsonSerializer[IO]
  ): IO[MptStore[IO, GlobalStateKey]] =
    InMemoryMerklePatriciaProducer.make[IO]().flatMap(MptStore.make[IO, GlobalStateKey](_, GlobalStateKey.toHex[IO]))

  private def sameBytes(left: Map[Hex, Array[Byte]], right: Map[Hex, Array[Byte]]): Boolean =
    left.keySet == right.keySet && left.forall { case (key, bytes) => right.get(key).exists(_.sameElements(bytes)) }

  private def isExactMalformed(result: Either[Throwable, Unit], expectedKey: Hex): Boolean =
    result match {
      case Left(error: StrictMptRead.MalformedConsensusMptValue) => error.physicalKey == expectedKey
      case _                                                    => false
    }

  test("store address-index RMW rejects malformed bytes without mutation") { res =>
    implicit val (hasher, json) = res

    for {
      store <- freshStore
      key <- GlobalStateKey.activeAddressIndexKey[IO](GlobalStateFieldId.Balances)
      hex <- GlobalStateKey.toHex[IO](key)
      _ <- store.underlying.insertBytes(Map(hex -> garbage)).flatMap(_.liftTo[IO])
      before <- store.allEntriesAsBytes
      result <- GlobalStateConverter
        .applyActiveAddressIndexDelta[IO](store, GlobalStateFieldId.Balances, Set(addressA), Set.empty)
        .attempt
      after <- store.allEntriesAsBytes
    } yield expect.all(isExactMalformed(result, hex), sameBytes(before, after))
  }

  test("store address-pair-index RMW rejects malformed bytes without mutation") { res =>
    implicit val (hasher, json) = res

    for {
      store <- freshStore
      key <- GlobalStateKey.activeAddressIndexKey[IO](GlobalStateFieldId.TokenLockBalances)
      hex <- GlobalStateKey.toHex[IO](key)
      _ <- store.underlying.insertBytes(Map(hex -> garbage)).flatMap(_.liftTo[IO])
      before <- store.allEntriesAsBytes
      result <- GlobalStateConverter
        .applyAddressPairIndexDelta[IO](
          store,
          GlobalStateFieldId.TokenLockBalances,
          Set(addressA -> addressB),
          Set.empty
        )
        .attempt
      after <- store.allEntriesAsBytes
    } yield expect.all(isExactMalformed(result, hex), sameBytes(before, after))
  }

  test("store expiry RMW preflights all buckets and leaves earlier buckets untouched") { res =>
    implicit val (hasher, json) = res
    val epochA = EpochProgress(NonNegLong(10L))
    val epochB = EpochProgress(NonNegLong(11L))
    val expiryA = TokenLockExpiryKey(addressA, Hash("a" * 64))
    val expiryB = TokenLockExpiryKey(addressB, Hash("b" * 64))
    val delta = SystemIndexDelta.EpochBucket[TokenLockExpiryKey](
      adds = SortedMap(epochA -> Set(expiryA), epochB -> Set(expiryB))
    )

    for {
      store <- freshStore
      malformedKey <- GlobalStateKey.expiryIndexKey[IO](SystemNamespaceLabel.ExpiryIndexTokenLocks, epochB)
      malformedHex <- GlobalStateKey.toHex[IO](malformedKey)
      _ <- store.underlying.insertBytes(Map(malformedHex -> garbage)).flatMap(_.liftTo[IO])
      before <- store.allEntriesAsBytes
      result <- GlobalStateConverter
        .applySystemIndexDelta[IO, TokenLockExpiryKey](store, SystemNamespaceLabel.ExpiryIndexTokenLocks, delta)
        .attempt
      after <- store.allEntriesAsBytes
    } yield expect.all(isExactMalformed(result, malformedHex), sameBytes(before, after))
  }

  test("expiry materialization rejects malformed rooted bytes at the exact key") { res =>
    implicit val (hasher, json) = res
    val epoch = EpochProgress(NonNegLong(13L))

    for {
      store <- freshStore
      key <- GlobalStateKey.expiryIndexKey[IO](SystemNamespaceLabel.ExpiryIndexTokenLocks, epoch)
      hex <- GlobalStateKey.toHex[IO](key)
      _ <- store.underlying.insertBytes(Map(hex -> garbage)).flatMap(_.liftTo[IO])
      result <- store
        .getExpiryBucket[TokenLockExpiryKey](SystemNamespaceLabel.ExpiryIndexTokenLocks, epoch)
        .void
        .attempt
    } yield expect(isExactMalformed(result, hex))
  }

  test("raw replay rejects malformed address-index bytes at the exact key") { res =>
    implicit val (hasher, json) = res
    val acc = StateChangesAccumulator(balances = SortedMap(addressA -> Balance(NonNegLong(1L))))

    for {
      key <- GlobalStateKey.activeAddressIndexKey[IO](GlobalStateFieldId.Balances)
      hex <- GlobalStateKey.toHex[IO](key)
      input = Map(hex -> garbage)
      result <- GlobalStateConverter.toAccumulatorHexDelta[IO](acc, input).void.attempt
    } yield expect.all(isExactMalformed(result, hex), input(hex).sameElements(garbage))
  }

  test("raw replay rejects malformed address-pair-index bytes at the exact key") { res =>
    implicit val (hasher, json) = res
    val acc = StateChangesAccumulator(
      tokenLockBalances = SortedMap(addressA -> SortedMap(addressB -> Balance(NonNegLong(1L))))
    )

    for {
      key <- GlobalStateKey.activeAddressIndexKey[IO](GlobalStateFieldId.TokenLockBalances)
      hex <- GlobalStateKey.toHex[IO](key)
      input = Map(hex -> garbage)
      result <- GlobalStateConverter.toAccumulatorHexDelta[IO](acc, input).void.attempt
    } yield expect.all(isExactMalformed(result, hex), input(hex).sameElements(garbage))
  }

  test("raw replay rejects malformed expiry bytes at the exact key") { res =>
    implicit val (hasher, json) = res
    val epoch = EpochProgress(NonNegLong(12L))
    val expiry = TokenLockExpiryKey(addressA, Hash("c" * 64))
    val acc = StateChangesAccumulator(
      tokenLockExpiryIndex = SystemIndexDelta.EpochBucket(adds = SortedMap(epoch -> Set(expiry)))
    )

    for {
      key <- GlobalStateKey.expiryIndexKey[IO](SystemNamespaceLabel.ExpiryIndexTokenLocks, epoch)
      hex <- GlobalStateKey.toHex[IO](key)
      input = Map(hex -> garbage)
      result <- GlobalStateConverter.toAccumulatorHexDelta[IO](acc, input).void.attempt
    } yield expect.all(isExactMalformed(result, hex), input(hex).sameElements(garbage))
  }
}
