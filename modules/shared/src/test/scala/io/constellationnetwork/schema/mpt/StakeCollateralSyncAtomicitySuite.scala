package io.constellationnetwork.schema.mpt

import cats.data.NonEmptySet
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.{Amount, Balance}
import io.constellationnetwork.schema.delegatedStake._
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt.GlobalStateConverter.{StateChangesAccumulator, applyAccumulatorToGSI}
import io.constellationnetwork.schema.nodeCollateral._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.{GlobalSnapshotInfo, GlobalStateProofSelector, SnapshotOrdinal}
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.{Expectations, MutableIOSuite}

object StakeCollateralSyncAtomicitySuite extends MutableIOSuite {

  implicit val globalStateProofSelector: GlobalStateProofSelector =
    GlobalStateProofSelector(SnapshotOrdinal(NonNegLong(Long.MaxValue)))
  implicit val withdrawalTimeLimit: WithdrawalTimeLimit = WithdrawalTimeLimit.none

  type Res = (Hasher[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      implicit0(jsonSerializer: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      hasher = Hasher.forJson[IO]
    } yield (hasher, jsonSerializer)

  private val source = Address.fromBytes("atomic-stake-source".getBytes("UTF-8"))
  private val balanceOwner = Address.fromBytes("atomic-balance-owner".getBytes("UTF-8"))
  private val nodeId = PeerId(Hex("11" * 64))
  private val seedOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(40L))
  private val rejectedOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(41L))

  private def proof(idByte: String, signatureByte: String): SignatureProof =
    SignatureProof(Id(Hex(idByte * 64)), Signature(Hex(signatureByte * 64)))

  private val proofsA = NonEmptySet.one(proof("12", "34"))
  private val proofsB = NonEmptySet.one(proof("56", "78"))

  private val delegatedCreate = UpdateDelegatedStake.Create(
    source,
    nodeId,
    DelegatedStakeAmount(NonNegLong.unsafeFrom(10L)),
    DelegatedStakeFee(NonNegLong.unsafeFrom(1L)),
    Hash("55" * 32),
    DelegatedStakeReference.empty
  )

  private val collateralCreate = UpdateNodeCollateral.Create(
    source,
    nodeId,
    NodeCollateralAmount(NonNegLong.unsafeFrom(20L)),
    NodeCollateralFee(NonNegLong.unsafeFrom(1L)),
    Hash("66" * 32),
    NodeCollateralReference.empty
  )

  private def ordinal(value: Long): SnapshotOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(value))
  private def epoch(value: Long): EpochProgress = EpochProgress(NonNegLong.unsafeFrom(value))

  private final case class MalformedCase(fieldId: GlobalStateFieldId, accumulator: StateChangesAccumulator)

  private val malformedCases = List(
    MalformedCase(
      GlobalStateFieldId.ActiveDelegatedStakes,
      StateChangesAccumulator(
        activeDelegatedStakes = SortedMap(
          source -> SortedSet(
            DelegatedStakeRecord(Signed(delegatedCreate, proofsA), ordinal(1L), Amount.empty),
            DelegatedStakeRecord(Signed(delegatedCreate, proofsB), ordinal(2L), Amount.empty)
          )
        )
      )
    ),
    MalformedCase(
      GlobalStateFieldId.DelegatedStakesWithdrawals,
      StateChangesAccumulator(
        delegatedStakesWithdrawals = SortedMap(
          source -> SortedSet(
            PendingDelegatedStakeWithdrawal(Signed(delegatedCreate, proofsA), Amount.empty, ordinal(1L), epoch(1L)),
            PendingDelegatedStakeWithdrawal(Signed(delegatedCreate, proofsB), Amount.empty, ordinal(2L), epoch(2L))
          )
        )
      )
    ),
    MalformedCase(
      GlobalStateFieldId.ActiveNodeCollaterals,
      StateChangesAccumulator(
        activeNodeCollaterals = SortedMap(
          source -> SortedSet(
            NodeCollateralRecord(Signed(collateralCreate, proofsA), ordinal(1L)),
            NodeCollateralRecord(Signed(collateralCreate, proofsB), ordinal(2L))
          )
        )
      )
    ),
    MalformedCase(
      GlobalStateFieldId.NodeCollateralWithdrawals,
      StateChangesAccumulator(
        nodeCollateralWithdrawals = SortedMap(
          source -> SortedSet(
            PendingNodeCollateralWithdrawal(Signed(collateralCreate, proofsA), ordinal(1L), epoch(1L)),
            PendingNodeCollateralWithdrawal(Signed(collateralCreate, proofsB), ordinal(2L), epoch(2L))
          )
        )
      )
    )
  )

  private def sameBytes(left: Map[Hex, Array[Byte]], right: Map[Hex, Array[Byte]]): Boolean =
    left.keySet == right.keySet && left.forall { case (key, bytes) => right.get(key).exists(_.sameElements(bytes)) }

  private def runCase(
    malformed: MalformedCase,
    sync: (MptStore[IO, GlobalStateKey], StateChangesAccumulator) => IO[Unit]
  )(implicit hasher: Hasher[IO], jsonSerializer: JsonSerializer[IO]): IO[Expectations] =
    for {
      producer <- InMemoryMerklePatriciaProducer.make[IO]()
      store <- MptStore.make[IO, GlobalStateKey](producer, GlobalStateKey.toHex[IO])
      _ <- store.syncFromStateChanges(
        StateChangesAccumulator(balances = SortedMap(balanceOwner -> Balance(NonNegLong.unsafeFrom(99L)))),
        seedOrdinal
      )
      beforeBytes <- store.allEntriesAsBytes
      beforeRoot <- producer.getCurrentRootHash
      beforePersistedOrdinal <- store.lastPersistedOrdinal
      beforeBuiltOrdinal <- producer.getLastBuiltOrdinal
      result <- sync(store, malformed.accumulator).attempt
      afterBytes <- store.allEntriesAsBytes
      afterRoot <- producer.getCurrentRootHash
      afterPersistedOrdinal <- store.lastPersistedOrdinal
      afterBuiltOrdinal <- producer.getLastBuiltOrdinal
    } yield
      expect.all(
        result.left.exists(error =>
          error.getMessage.contains(s"field-${malformed.fieldId.toInt}") &&
            error.getMessage.contains("duplicate unsigned create identity")
        ),
        beforeBytes.nonEmpty,
        beforeRoot.nonEmpty,
        beforePersistedOrdinal.contains(seedOrdinal),
        beforeBuiltOrdinal.contains(seedOrdinal),
        sameBytes(beforeBytes, afterBytes),
        beforeRoot == afterRoot,
        beforePersistedOrdinal == afterPersistedOrdinal,
        beforeBuiltOrdinal == afterBuiltOrdinal
      )

  test("syncFromStateChanges rejects duplicate stake/collateral identities without changing bytes, root, or ordinal") { res =>
    implicit val (hasher, jsonSerializer) = res

    malformedCases
      .traverse(runCase(_, (store, accumulator) => store.syncFromStateChanges(accumulator, rejectedOrdinal)))
      .map(_.reduce(_.and(_)))
  }

  test("syncFromGlobalSnapshotInfo rejects duplicate stake/collateral identities before replacement") { res =>
    implicit val (hasher, jsonSerializer) = res

    malformedCases
      .traverse(
        runCase(
          _,
          (store, accumulator) =>
            store.syncFromGlobalSnapshotInfo(applyAccumulatorToGSI(GlobalSnapshotInfo.empty, accumulator), rejectedOrdinal)
        )
      )
      .map(_.reduce(_.and(_)))
  }
}
