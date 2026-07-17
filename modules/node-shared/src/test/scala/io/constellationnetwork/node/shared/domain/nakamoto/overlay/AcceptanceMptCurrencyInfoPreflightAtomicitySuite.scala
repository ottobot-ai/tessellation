package io.constellationnetwork.node.shared.domain.nakamoto.overlay

import cats.data.NonEmptySet
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.schema.currency.{CurrencyIncrementalSnapshot, CurrencySnapshotInfo, CurrencySnapshotStateProof}
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.ParentChildTree
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.{Amount, Balance}
import io.constellationnetwork.schema.delegatedStake._
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.mpt.GlobalStateConverter.StateChangesAccumulator
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.schema.nodeCollateral._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.{GlobalStateProofSelector, SnapshotOrdinal, SnapshotTips}
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

object AcceptanceMptCurrencyInfoPreflightAtomicitySuite extends MutableIOSuite {

  implicit val stateProofSelector: GlobalStateProofSelector =
    GlobalStateProofSelector(SnapshotOrdinal(NonNegLong(Long.MaxValue)))

  type Res = (Hasher[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      implicit0(json: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      hasher = Hasher.forJson[IO]
    } yield (hasher, json)

  private def address(label: String): Address = Address.fromBytes(label.getBytes("UTF-8"))

  private val orderedMetagraphs = List(address("currency-preflight-mg-a"), address("currency-preflight-mg-b")).sorted
  private val firstMetagraph = orderedMetagraphs.head
  private val secondMetagraph = orderedMetagraphs.last
  private val holder = address("currency-preflight-holder")
  private val existingOwner = address("currency-preflight-existing")
  private val unrelatedOwner = address("currency-preflight-unrelated")
  private val parent = BranchId(Hash("0" * 64))
  private val ordinal = SnapshotOrdinal(NonNegLong(1L))

  private val proof = SignatureProof(Id(Hex("33" * 64)), Signature(Hex("44" * 70)))
  private val alternateProof = SignatureProof(Id(Hex("55" * 64)), Signature(Hex("66" * 70)))

  private def incremental(snapshotOrdinal: Long): Signed[CurrencyIncrementalSnapshot] =
    Signed(
      CurrencyIncrementalSnapshot(
        ordinal = SnapshotOrdinal.unsafeApply(snapshotOrdinal),
        height = Height.MinValue,
        subHeight = SubHeight.MinValue,
        lastSnapshotHash = Hash.empty,
        blocks = SortedSet.empty,
        rewards = SortedSet.empty,
        tips = SnapshotTips(SortedSet.empty, SortedSet.empty),
        stateProof = CurrencySnapshotStateProof(Hash.empty, Hash.empty, None, None, None, None, None, None, None),
        epochProgress = EpochProgress.MinValue,
        dataApplication = None,
        messages = None,
        globalSnapshotSyncs = None,
        feeTransactions = None,
        artifacts = None,
        allowSpendBlocks = None,
        tokenLockBlocks = None,
        globalSyncView = None
      ),
      NonEmptySet.one(proof)
    )

  private val emptyInfo = CurrencySnapshotInfo(
    lastTxRefs = SortedMap.empty,
    balances = SortedMap.empty,
    lastMessages = None,
    lastFeeTxRefs = None,
    lastAllowSpendRefs = None,
    activeAllowSpends = None,
    globalSnapshotSyncView = None,
    lastTokenLockRefs = None,
    activeTokenLocks = None
  )

  private val validFirstInfo =
    emptyInfo.copy(balances = SortedMap(holder -> Balance(NonNegLong(5L))))

  private val invalidSecondInfo =
    emptyInfo.copy(activeTokenLocks = Some(SortedMap(holder -> SortedSet.empty)))

  private def sameBytes(left: Map[Hex, Array[Byte]], right: Map[Hex, Array[Byte]]): Boolean =
    left.keySet == right.keySet && left.forall { case (key, bytes) => right.get(key).exists(_.sameElements(bytes)) }

  private def snapshotBytes(store: MptStore[IO, GlobalStateKey]): IO[Map[Hex, Array[Byte]]] =
    store.allEntriesAsBytes.map(_.map { case (key, bytes) => key -> bytes.clone() })

  private val stakeSource = address("stake-collateral-preflight-source")
  private val stakeNodeId = PeerId(Hex("77" * 64))
  private val stakeProofsA = NonEmptySet.one(proof)
  private val stakeProofsB = NonEmptySet.one(alternateProof)

  private val delegatedCreate = UpdateDelegatedStake.Create(
    stakeSource,
    stakeNodeId,
    DelegatedStakeAmount(NonNegLong.unsafeFrom(10L)),
    DelegatedStakeFee(NonNegLong.unsafeFrom(1L)),
    Hash("88" * 32),
    DelegatedStakeReference.empty
  )

  private val collateralCreate = UpdateNodeCollateral.Create(
    stakeSource,
    stakeNodeId,
    NodeCollateralAmount(NonNegLong.unsafeFrom(20L)),
    NodeCollateralFee(NonNegLong.unsafeFrom(1L)),
    Hash("99" * 32),
    NodeCollateralReference.empty
  )

  private def snapshotOrdinal(value: Long): SnapshotOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(value))
  private def epoch(value: Long): EpochProgress = EpochProgress(NonNegLong.unsafeFrom(value))

  private final case class MalformedStakeCase(fieldId: GlobalStateFieldId, accumulator: StateChangesAccumulator)

  private val malformedStakeCases = List(
    MalformedStakeCase(
      GlobalStateFieldId.ActiveDelegatedStakes,
      StateChangesAccumulator(
        activeDelegatedStakes = SortedMap(
          stakeSource -> SortedSet(
            DelegatedStakeRecord(Signed(delegatedCreate, stakeProofsA), snapshotOrdinal(1L), Amount.empty),
            DelegatedStakeRecord(Signed(delegatedCreate, stakeProofsB), snapshotOrdinal(2L), Amount.empty)
          )
        )
      )
    ),
    MalformedStakeCase(
      GlobalStateFieldId.DelegatedStakesWithdrawals,
      StateChangesAccumulator(
        delegatedStakesWithdrawals = SortedMap(
          stakeSource -> SortedSet(
            PendingDelegatedStakeWithdrawal(Signed(delegatedCreate, stakeProofsA), Amount.empty, snapshotOrdinal(1L), epoch(1L)),
            PendingDelegatedStakeWithdrawal(Signed(delegatedCreate, stakeProofsB), Amount.empty, snapshotOrdinal(2L), epoch(2L))
          )
        )
      )
    ),
    MalformedStakeCase(
      GlobalStateFieldId.ActiveNodeCollaterals,
      StateChangesAccumulator(
        activeNodeCollaterals = SortedMap(
          stakeSource -> SortedSet(
            NodeCollateralRecord(Signed(collateralCreate, stakeProofsA), snapshotOrdinal(1L)),
            NodeCollateralRecord(Signed(collateralCreate, stakeProofsB), snapshotOrdinal(2L))
          )
        )
      )
    ),
    MalformedStakeCase(
      GlobalStateFieldId.NodeCollateralWithdrawals,
      StateChangesAccumulator(
        nodeCollateralWithdrawals = SortedMap(
          stakeSource -> SortedSet(
            PendingNodeCollateralWithdrawal(Signed(collateralCreate, stakeProofsA), snapshotOrdinal(1L), epoch(1L)),
            PendingNodeCollateralWithdrawal(Signed(collateralCreate, stakeProofsB), snapshotOrdinal(2L), epoch(2L))
          )
        )
      )
    )
  )

  private def runMalformedStakeCase(
    malformed: MalformedStakeCase
  )(implicit hasher: Hasher[IO], json: JsonSerializer[IO]): IO[weaver.Expectations] =
    for {
      producer <- InMemoryMerklePatriciaProducer.make[IO]()
      store <- MptStore.make[IO, GlobalStateKey](producer, GlobalStateKey.toHex[IO])
      _ <- store.syncFromStateChanges(
        StateChangesAccumulator(balances = SortedMap(unrelatedOwner -> Balance(NonNegLong.unsafeFrom(9L)))),
        ordinal
      )
      beforeBytes <- snapshotBytes(store)
      beforeRoot <- producer.getCurrentRootHash
      beforePersistedOrdinal <- store.lastPersistedOrdinal
      beforeBuiltOrdinal <- producer.getLastBuiltOrdinal
      pcTree <- ParentChildTree.make[IO]
      overlay = MptOverlay.passthrough[IO, GlobalStateKey](store, pcTree)
      handle <- overlay.checkout(parent)
      mpt = AcceptanceMpt.fromOverlay[IO](overlay, parent, handle)
      result <- AcceptanceMptStateChanges.applyStateChanges[IO](mpt, malformed.accumulator).attempt
      afterBytes <- snapshotBytes(store)
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
        beforePersistedOrdinal.contains(ordinal),
        beforeBuiltOrdinal.contains(ordinal),
        sameBytes(beforeBytes, afterBytes),
        beforeRoot == afterRoot,
        beforePersistedOrdinal == afterPersistedOrdinal,
        beforeBuiltOrdinal == afterBuiltOrdinal
      )

  test("Passthrough rejects a later invalid currency info before mutating base bytes or root") { res =>
    implicit val (hasher, json) = res
    type CurrencyArm = Either[
      Signed[io.constellationnetwork.currency.schema.currency.CurrencySnapshot],
      (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)
    ]

    val acc = StateChangesAccumulator(
      balances = SortedMap(unrelatedOwner -> Balance(NonNegLong(9L))),
      lastCurrencySnapshots = SortedMap[Address, CurrencyArm](
        firstMetagraph -> Right((incremental(1L), validFirstInfo)),
        secondMetagraph -> Right((incremental(2L), invalidSecondInfo))
      )
    )

    for {
      producer <- InMemoryMerklePatriciaProducer.make[IO]()
      store <- MptStore.make[IO, GlobalStateKey](producer, GlobalStateKey.toHex[IO])
      _ <- store.insert[Balance](
        GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, existingOwner),
        Balance(NonNegLong(11L))
      )
      beforeBytes <- snapshotBytes(store)
      beforeRoot <- store.build(ordinal).map(_.toOption.map(_.rootHash))
      pcTree <- ParentChildTree.make[IO]
      overlay = MptOverlay.passthrough[IO, GlobalStateKey](store, pcTree)
      handle <- overlay.checkout(parent)
      mpt = AcceptanceMpt.fromOverlay[IO](overlay, parent, handle)
      result <- AcceptanceMptStateChanges.applyStateChanges[IO](mpt, acc).attempt
      afterBytes <- snapshotBytes(store)
      afterRoot <- store.build(ordinal).map(_.toOption.map(_.rootHash))
    } yield
      expect.all(
        result.left.exists {
          case error: StrictMptRead.InconsistentConsensusMptIndex => error.getMessage.contains("empty token-lock set")
          case _                                                  => false
        },
        beforeRoot.nonEmpty,
        sameBytes(beforeBytes, afterBytes),
        beforeRoot == afterRoot
      )
  }

  test("Passthrough rejects duplicate stake/collateral identities without changing bytes, root, or ordinal") { res =>
    implicit val (hasher, json) = res

    malformedStakeCases.traverse(runMalformedStakeCase).map(_.reduce(_.and(_)))
  }
}
