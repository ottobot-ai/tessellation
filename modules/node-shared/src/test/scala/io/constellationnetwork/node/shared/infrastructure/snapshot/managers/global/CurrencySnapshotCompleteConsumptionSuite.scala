package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.currency.schema.currency._
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.statechannel.StateChannelAcceptanceResult
import io.constellationnetwork.node.shared.domain.statechannel.StateChannelAcceptanceResult.CurrencySnapshotWithState
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.GlobalSnapshotStateChannelEventsProcessor.{
  CurrencyWindowConsumption,
  MetagraphAcceptanceResult
}
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, SnapshotOrdinal}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.security.{Hashed, Hasher}
import io.constellationnetwork.statechannel.{StateChannelOutput, StateChannelSnapshotBinary, StateChannelValidationType}

import weaver.MutableIOSuite

object CurrencySnapshotCompleteConsumptionSuite extends MutableIOSuite {

  override type Res = Hasher[IO]

  override def sharedResource: Resource[IO, Res] =
    Resource.eval(JsonSerializer.forAsync[IO]).map(implicit json => Hasher.forJson[IO])

  private def binary(
    label: String,
    parent: Hash = Hash.empty,
    proofByte: String = "11"
  ): Signed[StateChannelSnapshotBinary] = {
    val proof = SignatureProof(Id(Hex(proofByte * 64)), Signature(Hex("22" * 70)))
    Signed(
      StateChannelSnapshotBinary(parent, label.getBytes("UTF-8"), SnapshotFee.MinValue),
      NonEmptySet.one(proof)
    )
  }

  private def recordingProcessor(
    calls: Ref[IO, List[Set[Address]]],
    result: SortedMap[Address, MetagraphAcceptanceResult]
  ): GlobalSnapshotStateChannelEventsProcessor[IO] =
    new GlobalSnapshotStateChannelEventsProcessor[IO] {
      def process(
        snapshotOrdinal: SnapshotOrdinal,
        currentBalances: SortedMap[Address, Balance],
        priorLastStateChannelSnapshotHashes: SortedMap[Address, Hash],
        priorLastCurrencySnapshots: SortedMap[Address, Either[Signed[
          CurrencySnapshot
        ], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]],
        events: List[StateChannelOutput],
        validationType: StateChannelValidationType,
        getGlobalSnapshotByOrdinal: SnapshotOrdinal => IO[Option[Hashed[GlobalIncrementalSnapshot]]]
      )(implicit hasher: Hasher[IO]): IO[StateChannelAcceptanceResult] =
        IO.raiseError(new IllegalStateException("unexpected process call"))

      def processCurrencySnapshots(
        snapshotOrdinal: SnapshotOrdinal,
        currentBalances: SortedMap[Address, Balance],
        priorLastCurrencySnapshots: SortedMap[Address, Either[Signed[
          CurrencySnapshot
        ], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]],
        events: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
        getGlobalSnapshotByOrdinal: SnapshotOrdinal => IO[Option[Hashed[GlobalIncrementalSnapshot]]]
      )(implicit hasher: Hasher[IO]): IO[SortedMap[Address, MetagraphAcceptanceResult]] =
        calls.update(_ :+ events.keySet) *> IO.pure(result.filter { case (address, _) => events.contains(address) })

      def assembleAcceptanceResult(
        processed: SortedMap[Address, MetagraphAcceptanceResult],
        priorLastCurrencySnapshots: SortedMap[Address, Either[Signed[
          CurrencySnapshot
        ], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]],
        returned: Set[StateChannelOutput]
      ): StateChannelAcceptanceResult =
        throw new IllegalStateException("unexpected assembly call")
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

  test("outer lineage binds the pinned first parent and every canonical value-hash edge") { hasher =>
    implicit val h: Hasher[IO] = hasher

    val address = Address.fromBytes("lineage-window-mg".getBytes("UTF-8"))
    val otherAddress = Address.fromBytes("lineage-window-other".getBytes("UTF-8"))
    val base = Hash("ab" * 32)
    val wrong = Hash("cd" * 32)

    for {
      _ <- IO.unit
      genesis = binary("genesis")
      genesisResult <- GlobalSnapshotStateChannelEventsProcessor.classifyCheckpointLineage[IO](
        SortedMap(address -> Hash.empty),
        SortedMap(address -> NonEmptyList.one(genesis))
      )
      first = binary("first", base)
      firstValueHash <- Hasher[IO].hash(first.value)
      firstEnvelopeHash <- Hasher[IO].hash(first)
      second = binary("second", firstValueHash)
      secondValueHash <- Hasher[IO].hash(second.value)
      third = binary("third", secondValueHash)
      validSingleton <- GlobalSnapshotStateChannelEventsProcessor.classifyCheckpointLineage[IO](
        SortedMap(address -> base),
        SortedMap(address -> NonEmptyList.one(first))
      )
      validMulti <- GlobalSnapshotStateChannelEventsProcessor.classifyCheckpointLineage[IO](
        SortedMap(address -> base),
        SortedMap(address -> NonEmptyList.of(first, second, third))
      )
      wrongFirst <- GlobalSnapshotStateChannelEventsProcessor.classifyCheckpointLineage[IO](
        SortedMap(address -> wrong),
        SortedMap(address -> NonEmptyList.one(first))
      )
      reversed <- GlobalSnapshotStateChannelEventsProcessor.classifyCheckpointLineage[IO](
        SortedMap(address -> base),
        SortedMap(address -> NonEmptyList.of(second, first))
      )
      omitted <- GlobalSnapshotStateChannelEventsProcessor.classifyCheckpointLineage[IO](
        SortedMap(address -> base),
        SortedMap(address -> NonEmptyList.one(second))
      )
      duplicated <- GlobalSnapshotStateChannelEventsProcessor.classifyCheckpointLineage[IO](
        SortedMap(address -> base),
        SortedMap(address -> NonEmptyList.of(first, first))
      )
      badInternal = binary("bad-internal", wrong)
      internalWrong <- GlobalSnapshotStateChannelEventsProcessor.classifyCheckpointLineage[IO](
        SortedMap(address -> base),
        SortedMap(address -> NonEmptyList.of(first, badInternal))
      )
      substitute = binary("substitute", firstValueHash)
      substituted <- GlobalSnapshotStateChannelEventsProcessor.classifyCheckpointLineage[IO](
        SortedMap(address -> base),
        SortedMap(address -> NonEmptyList.of(first, substitute, third))
      )
      proofOnlyMutation = binary("first", base, proofByte = "33")
      proofOnly <- GlobalSnapshotStateChannelEventsProcessor.classifyCheckpointLineage[IO](
        SortedMap(address -> base),
        SortedMap(address -> NonEmptyList.of(proofOnlyMutation, second))
      )
      envelopeParent = binary("envelope-parent", firstEnvelopeHash)
      envelopeHashRejected <- GlobalSnapshotStateChannelEventsProcessor.classifyCheckpointLineage[IO](
        SortedMap(address -> base),
        SortedMap(address -> NonEmptyList.of(first, envelopeParent))
      )
      missing <- GlobalSnapshotStateChannelEventsProcessor.classifyCheckpointLineage[IO](
        SortedMap.empty,
        SortedMap(address -> NonEmptyList.one(first))
      )
      otherFirst = binary("other-first", base)
      isolated <- GlobalSnapshotStateChannelEventsProcessor.classifyCheckpointLineage[IO](
        SortedMap(address -> base, otherAddress -> base),
        SortedMap(address -> NonEmptyList.one(badInternal), otherAddress -> NonEmptyList.one(otherFirst))
      )
    } yield
      expect.all(
        genesisResult.isEmpty,
        validSingleton.isEmpty,
        validMulti.isEmpty,
        wrongFirst.get(address).contains(CurrencyWindowConsumption.InvalidOuterParentLink(0, wrong, base)),
        reversed.get(address).contains(CurrencyWindowConsumption.InvalidOuterParentLink(0, base, firstValueHash)),
        omitted.get(address).contains(CurrencyWindowConsumption.InvalidOuterParentLink(0, base, firstValueHash)),
        duplicated.get(address).contains(CurrencyWindowConsumption.InvalidOuterParentLink(1, firstValueHash, base)),
        internalWrong.get(address).contains(CurrencyWindowConsumption.InvalidOuterParentLink(1, firstValueHash, wrong)),
        substituted.get(address).exists {
          case CurrencyWindowConsumption.InvalidOuterParentLink(2, _, actual) => actual === secondValueHash
          case _                                                              => false
        },
        proofOnly.isEmpty,
        envelopeHashRejected
          .get(address)
          .contains(CurrencyWindowConsumption.InvalidOuterParentLink(1, firstValueHash, firstEnvelopeHash)),
        missing.get(address).contains(CurrencyWindowConsumption.MissingExpectedParent),
        isolated.keySet == Set(address)
      )
  }

  test("malformed lineage is rejected before currency recreation and does not suppress an independent metagraph") { hasher =>
    implicit val h: Hasher[IO] = hasher

    val invalidAddress = Address.fromBytes("invalid-lineage-mg".getBytes("UTF-8"))
    val validAddress = Address.fromBytes("valid-lineage-mg".getBytes("UTF-8"))
    val base = Hash("44" * 32)
    val malformed = binary("malformed", Hash("55" * 32))
    val valid = binary("valid", base)

    for {
      calls <- Ref.of[IO, List[Set[Address]]](List.empty)
      processor = recordingProcessor(calls, processed(validAddress, NonEmptyList.one(valid)))
      onlyInvalid <- processor.processCurrencySnapshotsWithCompleteConsumption(
        SnapshotOrdinal.MinValue,
        SortedMap.empty,
        SortedMap.empty,
        SortedMap(invalidAddress -> base),
        SortedMap(invalidAddress -> NonEmptyList.one(malformed)),
        _ => none.pure[IO]
      )
      afterInvalid <- calls.get
      mixed <- processor.processCurrencySnapshotsWithCompleteConsumption(
        SnapshotOrdinal.MinValue,
        SortedMap.empty,
        SortedMap.empty,
        SortedMap(invalidAddress -> base, validAddress -> base),
        SortedMap(invalidAddress -> NonEmptyList.one(malformed), validAddress -> NonEmptyList.one(valid)),
        _ => none.pure[IO]
      )
      afterMixed <- calls.get
    } yield
      expect.all(
        afterInvalid.isEmpty,
        onlyInvalid.completeResults.isEmpty,
        onlyInvalid.consumption(invalidAddress).exists(_.isInstanceOf[CurrencyWindowConsumption.InvalidOuterParentLink]),
        afterMixed == List(Set(validAddress)),
        mixed.completeResults.keySet == Set(validAddress),
        mixed.consumption(invalidAddress).exists(_.isInstanceOf[CurrencyWindowConsumption.InvalidOuterParentLink])
      )
  }
}
