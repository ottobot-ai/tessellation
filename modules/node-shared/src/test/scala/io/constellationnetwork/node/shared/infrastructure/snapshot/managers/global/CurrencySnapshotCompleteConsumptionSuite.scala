package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{IO, Resource}

import scala.collection.immutable.SortedMap

import io.constellationnetwork.currency.schema.currency.SnapshotFee
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.statechannel.StateChannelAcceptanceResult.CurrencySnapshotWithState
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.GlobalSnapshotStateChannelEventsProcessor.{
  CurrencyWindowConsumption,
  MetagraphAcceptanceResult
}
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import weaver.MutableIOSuite

object CurrencySnapshotCompleteConsumptionSuite extends MutableIOSuite {

  override type Res = Hasher[IO]

  override def sharedResource: Resource[IO, Res] =
    Resource.eval(JsonSerializer.forAsync[IO]).map { implicit json => Hasher.forJson[IO] }

  private def binary(label: String): Signed[StateChannelSnapshotBinary] = {
    val proof = SignatureProof(Id(Hex("11" * 64)), Signature(Hex("22" * 70)))
    Signed(
      StateChannelSnapshotBinary(Hash.empty, label.getBytes("UTF-8"), SnapshotFee.MinValue),
      NonEmptySet.one(proof)
    )
  }

  private def processed(
    address: Address,
    binaries: NonEmptyList[Signed[StateChannelSnapshotBinary]]
  ): SortedMap[Address, MetagraphAcceptanceResult] = {
    val pairs = binaries.map(binary => binary -> Option.empty[CurrencySnapshotWithState])
    SortedMap(address -> (pairs, SortedMap.empty[Address, Balance]))
  }

  test("exact signed-window classification rejects same-count reordering, substitution, and unexpected output") { hasher =>
    implicit val h: Hasher[IO] = hasher

    val address = Address.fromBytes("complete-window-mg".getBytes("UTF-8"))
    val unexpectedAddress = Address.fromBytes("unexpected-window-mg".getBytes("UTF-8"))
    val first = binary("first")
    val second = binary("second")
    val substitute = binary("substitute")
    val expected = SortedMap(address -> NonEmptyList.of(first, second))

    for {
      exact <- GlobalSnapshotStateChannelEventsProcessor.classifyCheckpointConsumption[IO](
        expected,
        processed(address, NonEmptyList.of(first, second))
      )
      reordered <- GlobalSnapshotStateChannelEventsProcessor.classifyCheckpointConsumption[IO](
        expected,
        processed(address, NonEmptyList.of(second, first))
      )
      substituted <- GlobalSnapshotStateChannelEventsProcessor.classifyCheckpointConsumption[IO](
        expected,
        processed(address, NonEmptyList.of(first, substitute))
      )
      withUnexpected <- GlobalSnapshotStateChannelEventsProcessor.classifyCheckpointConsumption[IO](
        expected,
        SortedMap.from(
          processed(address, NonEmptyList.of(first, second)).toList ++
            processed(unexpectedAddress, NonEmptyList.one(substitute)).toList
        )
      )
    } yield
      expect.all(
        exact.completeResult(address).nonEmpty,
        exact.allInputsCompletelyConsumed,
        reordered.completeResult(address).isEmpty,
        reordered.consumption(address).contains(CurrencyWindowConsumption.Incomplete(2, 2)),
        substituted.completeResult(address).isEmpty,
        substituted.consumption(address).contains(CurrencyWindowConsumption.Incomplete(2, 2)),
        !withUnexpected.allInputsCompletelyConsumed,
        withUnexpected.consumption(unexpectedAddress).contains(CurrencyWindowConsumption.UnexpectedOutput),
        withUnexpected.completeResults.keySet == Set(address)
      )
  }
}
