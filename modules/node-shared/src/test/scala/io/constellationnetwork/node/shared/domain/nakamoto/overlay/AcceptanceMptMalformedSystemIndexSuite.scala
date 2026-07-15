package io.constellationnetwork.node.shared.domain.nakamoto.overlay

import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.ParentChildTree
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.GlobalStateConverter.StateChangesAccumulator
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.schema.{GlobalStateProofSelector, SnapshotOrdinal}
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs._

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

object AcceptanceMptMalformedSystemIndexSuite extends MutableIOSuite {

  implicit val stateProofSelector: GlobalStateProofSelector =
    GlobalStateProofSelector(SnapshotOrdinal(NonNegLong(Long.MaxValue)))

  type Res = (Hasher[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      implicit0(json: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      hasher = Hasher.forJson[IO]
    } yield (hasher, json)

  private val garbage = Array[Byte](0x7f)
  private val addressA = Address.fromBytes("overlay-malformed-system-a".getBytes("UTF-8"))
  private val addressB = Address.fromBytes("overlay-malformed-system-b".getBytes("UTF-8"))
  private val parent = BranchId(Hash("0" * 64))

  private def sameBytes(left: Map[Hex, Array[Byte]], right: Map[Hex, Array[Byte]]): Boolean =
    left.keySet == right.keySet && left.forall { case (key, bytes) => right.get(key).exists(_.sameElements(bytes)) }

  private def runMalformed(
    acc: StateChangesAccumulator,
    keyF: IO[GlobalStateKey]
  )(implicit hasher: Hasher[IO], json: JsonSerializer[IO]): IO[(Either[Throwable, Unit], Hex, Boolean)] =
    for {
      producer <- InMemoryMerklePatriciaProducer.make[IO]()
      store <- MptStore.make[IO, GlobalStateKey](producer, GlobalStateKey.toHex[IO])
      key <- keyF
      hex <- GlobalStateKey.toHex[IO](key)
      _ <- producer.insertBytes(Map(hex -> garbage)).flatMap(_.liftTo[IO])
      before <- store.allEntriesAsBytes
      pcTree <- ParentChildTree.make[IO]
      overlay = MptOverlay.passthrough[IO, GlobalStateKey](store, pcTree)
      handle <- overlay.checkout(parent)
      mpt = AcceptanceMpt.fromOverlay[IO](overlay, parent, handle)
      result <- AcceptanceMptStateChanges.applyStateChanges[IO](mpt, acc).attempt
      after <- store.allEntriesAsBytes
    } yield (result, hex, sameBytes(before, after))

  private def expectMalformed(result: (Either[Throwable, Unit], Hex, Boolean)): weaver.Expectations = {
    val (outcome, expectedKey, unchanged) = result
    expect.all(
      outcome match {
        case Left(error: StrictMptRead.MalformedConsensusMptValue) => error.physicalKey == expectedKey
        case _                                                     => false
      },
      unchanged
    )
  }

  test("overlay address-index preflight rejects malformed bytes before staging any mutation") { res =>
    implicit val (hasher, json) = res
    val acc = StateChangesAccumulator(balances = SortedMap(addressA -> Balance(NonNegLong(1L))))

    runMalformed(acc, GlobalStateKey.activeAddressIndexKey[IO](GlobalStateFieldId.Balances)).map(expectMalformed)
  }

  test("overlay address-pair-index preflight rejects malformed bytes before staging any mutation") { res =>
    implicit val (hasher, json) = res
    val acc = StateChangesAccumulator(
      tokenLockBalances = SortedMap(addressA -> SortedMap(addressB -> Balance(NonNegLong(1L))))
    )

    runMalformed(acc, GlobalStateKey.activeAddressIndexKey[IO](GlobalStateFieldId.TokenLockBalances)).map(expectMalformed)
  }

  test("overlay expiry-index preflight rejects malformed bytes before staging any mutation") { res =>
    implicit val (hasher, json) = res
    val epoch = EpochProgress(NonNegLong(12L))
    val expiry = TokenLockExpiryKey(addressA, Hash("a" * 64))
    val acc = StateChangesAccumulator(
      tokenLockExpiryIndex = SystemIndexDelta.EpochBucket(adds = SortedMap(epoch -> Set(expiry)))
    )

    runMalformed(acc, GlobalStateKey.expiryIndexKey[IO](SystemNamespaceLabel.ExpiryIndexTokenLocks, epoch)).map(expectMalformed)
  }
}
