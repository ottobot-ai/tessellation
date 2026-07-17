package io.constellationnetwork.schema.mpt

import cats.data.NonEmptySet
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.delegatedStake._
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.GlobalStateConverter.{StateChangesAccumulator, applyAccumulatorToGSI}
import io.constellationnetwork.schema.nodeCollateral._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.{Expectations, MutableIOSuite}

object StakeCollateralWriterValidationSuite extends MutableIOSuite {

  implicit val globalStateProofSelector: GlobalStateProofSelector =
    GlobalStateProofSelector(SnapshotOrdinal(NonNegLong(Long.MaxValue)))
  implicit val withdrawalTimeLimit: WithdrawalTimeLimit = WithdrawalTimeLimit.none

  type Res = (Hasher[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      implicit0(jsonSerializer: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(hasher: Hasher[IO]) = Hasher.forJson[IO]
    } yield (hasher, jsonSerializer)

  private val sourceA = Address.fromBytes("writer-source-a".getBytes("UTF-8"))
  private val sourceB = Address.fromBytes("writer-source-b".getBytes("UTF-8"))
  private val nodeId = PeerId(Hex("11" * 64))

  private def proof(byte: String): SignatureProof =
    SignatureProof(Id(Hex(byte * 64)), Signature(Hex(byte.reverse * 64)))

  private val proofsA = NonEmptySet.one(proof("12"))
  private val proofsB = NonEmptySet.one(proof("34"))

  private def ordinal(value: Long): SnapshotOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(value))
  private def epoch(value: Long): EpochProgress = EpochProgress(NonNegLong.unsafeFrom(value))

  private def delegatedCreate(source: Address, amount: Long): UpdateDelegatedStake.Create =
    UpdateDelegatedStake.Create(
      source,
      nodeId,
      DelegatedStakeAmount(NonNegLong.unsafeFrom(amount)),
      DelegatedStakeFee(NonNegLong(1L)),
      Hash("55" * 32),
      DelegatedStakeReference.empty
    )

  private def collateralCreate(source: Address, amount: Long): UpdateNodeCollateral.Create =
    UpdateNodeCollateral.Create(
      source,
      nodeId,
      NodeCollateralAmount(NonNegLong.unsafeFrom(amount)),
      NodeCollateralFee(NonNegLong(1L)),
      Hash("66" * 32),
      NodeCollateralReference.empty
    )

  private def delegatedRecord(
    create: UpdateDelegatedStake.Create,
    proofs: NonEmptySet[SignatureProof],
    createdAt: Long
  ): DelegatedStakeRecord =
    DelegatedStakeRecord(Signed(create, proofs), ordinal(createdAt), balance.Amount.empty)

  private def delegatedWithdrawal(
    create: UpdateDelegatedStake.Create,
    proofs: NonEmptySet[SignatureProof],
    acceptedOrdinal: Long,
    createdAt: Long
  ): PendingDelegatedStakeWithdrawal =
    PendingDelegatedStakeWithdrawal(
      Signed(create, proofs),
      balance.Amount.empty,
      ordinal(acceptedOrdinal),
      epoch(createdAt)
    )

  private def collateralRecord(
    create: UpdateNodeCollateral.Create,
    proofs: NonEmptySet[SignatureProof],
    createdAt: Long
  ): NodeCollateralRecord =
    NodeCollateralRecord(Signed(create, proofs), ordinal(createdAt))

  private def collateralWithdrawal(
    create: UpdateNodeCollateral.Create,
    proofs: NonEmptySet[SignatureProof],
    acceptedOrdinal: Long,
    createdAt: Long
  ): PendingNodeCollateralWithdrawal =
    PendingNodeCollateralWithdrawal(Signed(create, proofs), ordinal(acceptedOrdinal), epoch(createdAt))

  private val delegatedA = delegatedCreate(sourceA, 10L)
  private val delegatedA2 = delegatedCreate(sourceA, 11L)
  private val delegatedB = delegatedCreate(sourceB, 12L)
  private val collateralA = collateralCreate(sourceA, 20L)
  private val collateralA2 = collateralCreate(sourceA, 21L)
  private val collateralB = collateralCreate(sourceB, 22L)

  private final case class WriterCase(name: String, accumulator: StateChangesAccumulator, expectedFailure: Option[String])

  private val activeDelegatedCases = List(
    WriterCase(
      "field-13 canonical",
      StateChangesAccumulator(activeDelegatedStakes = SortedMap(sourceA -> SortedSet(delegatedRecord(delegatedA, proofsA, 1L)))),
      none
    ),
    WriterCase(
      "field-13 empty",
      StateChangesAccumulator(activeDelegatedStakes = SortedMap(sourceA -> SortedSet.empty[DelegatedStakeRecord])),
      "empty record set".some
    ),
    WriterCase(
      "field-13 wrong source",
      StateChangesAccumulator(activeDelegatedStakes = SortedMap(sourceA -> SortedSet(delegatedRecord(delegatedB, proofsA, 1L)))),
      "source/key mismatch".some
    ),
    WriterCase(
      "field-13 mixed source",
      StateChangesAccumulator(
        activeDelegatedStakes = SortedMap(
          sourceA -> SortedSet(delegatedRecord(delegatedA, proofsA, 1L), delegatedRecord(delegatedB, proofsA, 2L))
        )
      ),
      "mixed embedded create sources".some
    ),
    WriterCase(
      "field-13 duplicate identity",
      StateChangesAccumulator(
        activeDelegatedStakes = SortedMap(
          sourceA -> SortedSet(delegatedRecord(delegatedA, proofsA, 1L), delegatedRecord(delegatedA, proofsB, 2L))
        )
      ),
      "duplicate unsigned create identity".some
    ),
    WriterCase(
      "field-13 same-time batch",
      StateChangesAccumulator(
        activeDelegatedStakes = SortedMap(
          sourceA -> SortedSet(delegatedRecord(delegatedA, proofsA, 1L), delegatedRecord(delegatedA2, proofsA, 1L))
        )
      ),
      none
    )
  )

  private val delegatedWithdrawalCases = List(
    WriterCase(
      "field-14 canonical",
      StateChangesAccumulator(
        delegatedStakesWithdrawals = SortedMap(sourceA -> SortedSet(delegatedWithdrawal(delegatedA, proofsA, 1L, 1L)))
      ),
      none
    ),
    WriterCase(
      "field-14 empty",
      StateChangesAccumulator(
        delegatedStakesWithdrawals = SortedMap(sourceA -> SortedSet.empty[PendingDelegatedStakeWithdrawal])
      ),
      "empty record set".some
    ),
    WriterCase(
      "field-14 wrong source",
      StateChangesAccumulator(
        delegatedStakesWithdrawals = SortedMap(sourceA -> SortedSet(delegatedWithdrawal(delegatedB, proofsA, 1L, 1L)))
      ),
      "source/key mismatch".some
    ),
    WriterCase(
      "field-14 mixed source",
      StateChangesAccumulator(
        delegatedStakesWithdrawals = SortedMap(
          sourceA -> SortedSet(
            delegatedWithdrawal(delegatedA, proofsA, 1L, 1L),
            delegatedWithdrawal(delegatedB, proofsA, 2L, 2L)
          )
        )
      ),
      "mixed embedded create sources".some
    ),
    WriterCase(
      "field-14 duplicate identity",
      StateChangesAccumulator(
        delegatedStakesWithdrawals = SortedMap(
          sourceA -> SortedSet(
            delegatedWithdrawal(delegatedA, proofsA, 1L, 1L),
            delegatedWithdrawal(delegatedA, proofsB, 2L, 2L)
          )
        )
      ),
      "duplicate unsigned create identity".some
    ),
    WriterCase(
      "field-14 same-time batch",
      StateChangesAccumulator(
        delegatedStakesWithdrawals = SortedMap(
          sourceA -> SortedSet(
            delegatedWithdrawal(delegatedA, proofsA, 1L, 1L),
            delegatedWithdrawal(delegatedA2, proofsA, 2L, 1L)
          )
        )
      ),
      none
    )
  )

  private val activeCollateralCases = List(
    WriterCase(
      "field-15 canonical",
      StateChangesAccumulator(activeNodeCollaterals = SortedMap(sourceA -> SortedSet(collateralRecord(collateralA, proofsA, 1L)))),
      none
    ),
    WriterCase(
      "field-15 empty",
      StateChangesAccumulator(activeNodeCollaterals = SortedMap(sourceA -> SortedSet.empty[NodeCollateralRecord])),
      "empty record set".some
    ),
    WriterCase(
      "field-15 wrong source",
      StateChangesAccumulator(activeNodeCollaterals = SortedMap(sourceA -> SortedSet(collateralRecord(collateralB, proofsA, 1L)))),
      "source/key mismatch".some
    ),
    WriterCase(
      "field-15 mixed source",
      StateChangesAccumulator(
        activeNodeCollaterals = SortedMap(
          sourceA -> SortedSet(collateralRecord(collateralA, proofsA, 1L), collateralRecord(collateralB, proofsA, 2L))
        )
      ),
      "mixed embedded create sources".some
    ),
    WriterCase(
      "field-15 duplicate identity",
      StateChangesAccumulator(
        activeNodeCollaterals = SortedMap(
          sourceA -> SortedSet(collateralRecord(collateralA, proofsA, 1L), collateralRecord(collateralA, proofsB, 2L))
        )
      ),
      "duplicate unsigned create identity".some
    ),
    WriterCase(
      "field-15 same-time batch",
      StateChangesAccumulator(
        activeNodeCollaterals = SortedMap(
          sourceA -> SortedSet(collateralRecord(collateralA, proofsA, 1L), collateralRecord(collateralA2, proofsA, 1L))
        )
      ),
      none
    )
  )

  private val collateralWithdrawalCases = List(
    WriterCase(
      "field-16 canonical",
      StateChangesAccumulator(
        nodeCollateralWithdrawals = SortedMap(sourceA -> SortedSet(collateralWithdrawal(collateralA, proofsA, 1L, 1L)))
      ),
      none
    ),
    WriterCase(
      "field-16 empty",
      StateChangesAccumulator(
        nodeCollateralWithdrawals = SortedMap(sourceA -> SortedSet.empty[PendingNodeCollateralWithdrawal])
      ),
      "empty record set".some
    ),
    WriterCase(
      "field-16 wrong source",
      StateChangesAccumulator(
        nodeCollateralWithdrawals = SortedMap(sourceA -> SortedSet(collateralWithdrawal(collateralB, proofsA, 1L, 1L)))
      ),
      "source/key mismatch".some
    ),
    WriterCase(
      "field-16 mixed source",
      StateChangesAccumulator(
        nodeCollateralWithdrawals = SortedMap(
          sourceA -> SortedSet(
            collateralWithdrawal(collateralA, proofsA, 1L, 1L),
            collateralWithdrawal(collateralB, proofsA, 2L, 2L)
          )
        )
      ),
      "mixed embedded create sources".some
    ),
    WriterCase(
      "field-16 duplicate identity",
      StateChangesAccumulator(
        nodeCollateralWithdrawals = SortedMap(
          sourceA -> SortedSet(
            collateralWithdrawal(collateralA, proofsA, 1L, 1L),
            collateralWithdrawal(collateralA, proofsB, 2L, 2L)
          )
        )
      ),
      "duplicate unsigned create identity".some
    ),
    WriterCase(
      "field-16 same-time batch",
      StateChangesAccumulator(
        nodeCollateralWithdrawals = SortedMap(
          sourceA -> SortedSet(
            collateralWithdrawal(collateralA, proofsA, 1L, 1L),
            collateralWithdrawal(collateralA2, proofsA, 2L, 1L)
          )
        )
      ),
      none
    )
  )

  private val cases = activeDelegatedCases ++ delegatedWithdrawalCases ++ activeCollateralCases ++ collateralWithdrawalCases

  private def runAllWriters(
    accumulator: StateChangesAccumulator
  )(implicit hasher: Hasher[IO], jsonSerializer: JsonSerializer[IO]): IO[List[Either[Throwable, Unit]]] = {
    val info = applyAccumulatorToGSI(GlobalSnapshotInfo.empty, accumulator)

    List(
      IO.defer(GlobalStateConverter.toStateKeyValuePairsFromAccumulator[IO](accumulator)).void.attempt,
      IO.defer(GlobalStateConverter.toAccumulatorBytesDelta[IO](accumulator)).void.attempt,
      IO.defer(GlobalStateConverter.toAllStateKeyValuePairs[IO](info)).void.attempt,
      IO.defer(GlobalStateConverter.toAllStateKeyValueBytes[IO](info)).void.attempt
    ).sequence
  }

  private def checkCase(writerCase: WriterCase)(implicit hasher: Hasher[IO], jsonSerializer: JsonSerializer[IO]): IO[Expectations] =
    runAllWriters(writerCase.accumulator).map { attempts =>
      writerCase.expectedFailure match {
        case None =>
          expect.all(
            attempts.size == 4,
            attempts.forall(_.isRight)
          )
        case Some(expected) =>
          expect.all(
            attempts.size == 4,
            attempts.forall(_.left.exists(_.getMessage.contains(expected)))
          )
      }
    }

  test("all four writer roots accept canonical singleton and same-time batch fields 13-16") { res =>
    implicit val (hasher, jsonSerializer) = res

    cases.filter(_.expectedFailure.isEmpty).traverse(checkCase).map(_.reduce(_.and(_)))
  }

  test("all four writer roots reject malformed fields 13-16 before emission") { res =>
    implicit val (hasher, jsonSerializer) = res

    cases.filter(_.expectedFailure.nonEmpty).traverse(checkCase).map(_.reduce(_.and(_)))
  }
}
