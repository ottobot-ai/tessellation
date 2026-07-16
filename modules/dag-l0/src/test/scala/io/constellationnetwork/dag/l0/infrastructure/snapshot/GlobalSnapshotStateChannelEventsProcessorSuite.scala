package io.constellationnetwork.dag.l0.infrastructure.snapshot

import java.security.KeyPair

import cats.Parallel
import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.std.Random
import cats.effect.{Async, IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.schema.currency._
import io.constellationnetwork.env.AppEnvironment.Dev
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.{JsonBrotliBinarySerializer, JsonSerializer}
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.config.types._
import io.constellationnetwork.node.shared.domain.statechannel._
import io.constellationnetwork.node.shared.domain.swap.block.AllowSpendBlockAcceptanceManager
import io.constellationnetwork.node.shared.domain.tokenlock.block.TokenLockBlockAcceptanceManager
import io.constellationnetwork.node.shared.infrastructure.block.processing.BlockAcceptanceManager
import io.constellationnetwork.node.shared.infrastructure.consensus.CurrencySnapshotEventValidationErrorStorage
import io.constellationnetwork.node.shared.infrastructure.snapshot._
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.currency.CurrencySnapshotAcceptanceManager
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.{
  GlobalSnapshotStateChannelAcceptanceManager,
  GlobalSnapshotStateChannelEventsProcessor
}
import io.constellationnetwork.node.shared.infrastructure.snapshot.storage.{LastNGlobalSnapshotStorage, LastSnapshotStorage}
import io.constellationnetwork.node.shared.modules.SharedValidators
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.{Amount, Balance}
import io.constellationnetwork.schema.currencyMessage.{CurrencyMessage, MessageOrdinal, MessageType}
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.schema.node.RewardFraction
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.{GlobalStateProofSelector, _}
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.Signed.forAsyncHasher
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.shared.sharedKryoRegistrar
import io.constellationnetwork.statechannel.{StateChannelOutput, StateChannelSnapshotBinary, StateChannelValidationType}

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.{NonNegLong, PosInt, PosLong}
import fs2.concurrent.SignallingRef
import weaver.MutableIOSuite
object GlobalSnapshotStateChannelEventsProcessorSuite extends MutableIOSuite {
  implicit val globalStateProofSelector: GlobalStateProofSelector = GlobalStateProofSelector(SnapshotOrdinal(NonNegLong(Long.MaxValue)))
  implicit val withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit =
    io.constellationnetwork.schema.mpt.WithdrawalTimeLimit.none

  val TestValidationErrorStorageMaxSize: PosInt = PosInt(16)

  type Res = (KryoSerializer[IO], Hasher[IO], JsonSerializer[IO], SecurityProvider[IO])

  final case class ProcessorHarness(
    processor: GlobalSnapshotStateChannelEventsProcessor[IO],
    creator: CurrencySnapshotCreator[IO],
    initialGlobalSnapshot: Hashed[GlobalIncrementalSnapshot]
  )

  def sharedResource: Resource[IO, Res] = for {
    implicit0(ks: KryoSerializer[IO]) <- KryoSerializer.forAsync[IO](sharedKryoRegistrar)
    sp <- SecurityProvider.forAsync[IO]
    implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
    h = Hasher.forJson[IO]
  } yield (ks, h, j, sp)

  def mkProcessorHarness(
    stateChannelAllowanceLists: Map[Address, NonEmptySet[PeerId]],
    failed: Option[(Address, StateChannelValidator.StateChannelValidationError)] = None,
    feeConfigs: SortedMap[SnapshotOrdinal, FeeCalculatorConfig] = SortedMap.empty,
    contextFnsOverride: Option[CurrencySnapshotContextFunctions[IO]] = None,
    validationOverride: Option[
      (StateChannelOutput, SnapshotFeesInfo) => StateChannelValidator.StateChannelValidationErrorOr[StateChannelOutput]
    ] = None
  )(implicit H: Hasher[IO], S: SecurityProvider[IO], J: JsonSerializer[IO], K: KryoSerializer[IO]) = {
    implicit val hs = HasherSelector.forSyncAlwaysCurrent(H)
    implicit val csps = CurrencyStateProofSelector.instance

    for {
      _ <- IO.unit
      validator = new StateChannelValidator[IO] {
        def validate(
          output: StateChannelOutput,
          globalOrdinal: SnapshotOrdinal,
          snapshotFeesInfo: SnapshotFeesInfo
        )(implicit hasher: Hasher[IO]) =
          IO.pure(
            validationOverride
              .map(_(output, snapshotFeesInfo))
              .getOrElse(failed.filter(f => f._1 == output.address).map(_._2.invalidNec).getOrElse(output.validNec))
          )
        def validateHistorical(output: StateChannelOutput, globalOrdinal: SnapshotOrdinal, snapshotFeesInfo: SnapshotFeesInfo)(
          implicit hasher: Hasher[IO]
        ) =
          validate(output, globalOrdinal, snapshotFeesInfo)(hasher)
      }

      validators = SharedValidators
        .make[IO](
          Dev,
          AddressesConfig(Set()),
          None,
          None,
          Some(stateChannelAllowanceLists),
          SortedMap.empty,
          Long.MaxValue,
          Hasher.forKryo[IO],
          DelegatedStakingConfig(
            RewardFraction(5_000_000),
            RewardFraction(10_000_000),
            PosInt(140),
            PosInt(10),
            PosLong((5000 * 1e8).toLong),
            Map(Dev -> EpochProgress(NonNegLong(7338977L)))
          ),
          PriceOracleConfig(None, NonNegLong(0))
        )
      lastNSnapR <- SignallingRef.of[IO, Option[(Hashed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)]](None)
      incLastNSnapR <- SignallingRef
        .of[IO, SortedMap[SnapshotOrdinal, Hashed[GlobalIncrementalSnapshot]]](SortedMap.empty)
      lastSnapR <- SignallingRef.of[IO, Option[(Hashed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)]](None)

      lastGlobalSnapshotsSyncConfig =
        LastGlobalSnapshotsSyncConfig(NonNegLong(2L), PosInt.unsafeFrom(5))
      lastNSnapshotStorage =
        LastNGlobalSnapshotStorage.make[IO](lastGlobalSnapshotsSyncConfig, lastNSnapR, incLastNSnapR)
      lastGlobalSnapshotStorage = LastSnapshotStorage.make[IO, GlobalIncrementalSnapshot, GlobalSnapshotInfo](lastSnapR)
      initialGlobalInfo = mkGlobalSnapshotInfo()
      initialGlobalSnapshot <- mkGlobalIncrementalSnapshot[IO](initialGlobalInfo)
      _ <- lastGlobalSnapshotStorage.setInitial(initialGlobalSnapshot, initialGlobalInfo)

      currencySnapshotAcceptanceManager <- CurrencySnapshotAcceptanceManager.make(
        FieldsAddedOrdinals(Map.empty, Map.empty, Map.empty, Map.empty, Map.empty, Map.empty, Map.empty, Map.empty),
        Dev,
        LastGlobalSnapshotsSyncConfig(NonNegLong(2L), PosInt(10)),
        BlockAcceptanceManager.make[IO](validators.currencyBlockValidator, Hasher.forKryo[IO]),
        TokenLockBlockAcceptanceManager.make[IO](validators.tokenLockBlockValidator),
        AllowSpendBlockAcceptanceManager.make[IO](validators.allowSpendBlockValidator),
        Amount(0L),
        validators.currencyMessageValidator,
        validators.feeTransactionValidator,
        validators.globalSnapshotSyncValidator,
        lastNSnapshotStorage,
        lastGlobalSnapshotStorage
      )
      currencyEventsCutter = CurrencyEventsCutter.make[IO](None)
      validationErrorStorage <- CurrencySnapshotEventValidationErrorStorage.make(TestValidationErrorStorageMaxSize)
      creator = CurrencySnapshotCreator
        .make[IO](
          SnapshotOrdinal.MinValue,
          currencySnapshotAcceptanceManager,
          None,
          SnapshotSizeConfig(1L, Long.MaxValue),
          currencyEventsCutter,
          validationErrorStorage
        )
      currencySnapshotValidator = CurrencySnapshotValidator
        .make[IO](creator, validators.signedValidator, None, None)
      mptProducer <- InMemoryMerklePatriciaProducer.make[IO]()
      mptStore <- MptStore.make[IO, GlobalStateKey](
        mptProducer,
        GlobalStateKey.toHex[IO]
      )

      defaultCurrencySnapshotContextFns = CurrencySnapshotContextFunctions.make(currencySnapshotValidator)
      currencySnapshotContextFns = contextFnsOverride.getOrElse(defaultCurrencySnapshotContextFns)
      manager = new GlobalSnapshotStateChannelAcceptanceManager[IO] {
        def accept(
          ordinal: SnapshotOrdinal,
          priorLastStateChannelSnapshotHashes: SortedMap[Address, Hash],
          events: List[StateChannelOutput]
        )(
          implicit hasher: Hasher[IO]
        ): IO[
          (
            SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
            Set[StateChannelOutput]
          )
        ] = IO.pure((events.groupByNel(_.address).map { case (k, v) => k -> v.map(_.snapshotBinary) }, Set.empty))
      }
      feeCalculator = FeeCalculator.make(feeConfigs)
      processor = GlobalSnapshotStateChannelEventsProcessor
        .make[IO](
          validator,
          manager,
          currencySnapshotContextFns,
          feeCalculator,
          io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader.fromMptStore(mptStore)
        )
    } yield ProcessorHarness(processor, creator, initialGlobalSnapshot)
  }

  def mkProcessor(
    stateChannelAllowanceLists: Map[Address, NonEmptySet[PeerId]],
    failed: Option[(Address, StateChannelValidator.StateChannelValidationError)] = None
  )(implicit H: Hasher[IO], S: SecurityProvider[IO], J: JsonSerializer[IO], K: KryoSerializer[IO]) =
    mkProcessorHarness(stateChannelAllowanceLists, failed).map(_.processor)

  test("return new sc event") { res =>
    implicit val (ks, h, j, sp) = res

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      address = keyPair.getPublic().toAddress
      output <- mkStateChannelOutput(keyPair)
      snapshotInfo = mkGlobalSnapshotInfo()
      snapshot <- mkGlobalIncrementalSnapshot[IO](snapshotInfo)
      service <- mkProcessor(Map(address -> output.snapshotBinary.proofs.map(_.id.toPeerId)))
      expected = StateChannelAcceptanceResult(
        SortedMap((address, NonEmptyList.one(output.snapshotBinary))),
        SortedMap.empty[Address, StateChannelAcceptanceResult.CurrencySnapshotWithState],
        Set.empty,
        SortedMap.empty,
        SortedMap.empty[Address, List[StateChannelAcceptanceResult.CurrencySnapshotWithState]]
      )
      result <- service.process(
        SnapshotOrdinal(1L),
        snapshotInfo.balances,
        snapshotInfo.lastStateChannelSnapshotHashes,
        snapshotInfo.lastCurrencySnapshots,
        output :: Nil,
        StateChannelValidationType.Full,
        _ => None.pure[IO]
      )
    } yield expect.eql(expected, result)

  }

  test("return sc events for different addresses") { res =>
    implicit val (ks, h, j, sp) = res

    for {
      keyPair1 <- KeyPairGenerator.makeKeyPair[IO]
      address1 = keyPair1.getPublic().toAddress
      output1 <- mkStateChannelOutput(keyPair1)
      keyPair2 <- KeyPairGenerator.makeKeyPair[IO]
      address2 = keyPair2.getPublic().toAddress
      output2 <- mkStateChannelOutput(keyPair2)
      snapshotInfo = mkGlobalSnapshotInfo()
      snapshot <- mkGlobalIncrementalSnapshot[IO](snapshotInfo)
      service <- mkProcessor(
        Map(address1 -> output1.snapshotBinary.proofs.map(_.id.toPeerId), address2 -> output2.snapshotBinary.proofs.map(_.id.toPeerId))
      )
      expected = StateChannelAcceptanceResult(
        SortedMap((address1, NonEmptyList.of(output1.snapshotBinary)), (address2, NonEmptyList.of(output2.snapshotBinary))),
        SortedMap.empty[Address, StateChannelAcceptanceResult.CurrencySnapshotWithState],
        Set.empty,
        SortedMap.empty,
        SortedMap.empty[Address, List[StateChannelAcceptanceResult.CurrencySnapshotWithState]]
      )
      result <- service.process(
        SnapshotOrdinal(1L),
        snapshotInfo.balances,
        snapshotInfo.lastStateChannelSnapshotHashes,
        snapshotInfo.lastCurrencySnapshots,
        output1 :: output2 :: Nil,
        StateChannelValidationType.Full,
        _ => None.pure[IO]
      )
    } yield expect.eql(expected, result)

  }

  test("return only valid sc events") { res =>
    implicit val (ks, h, j, sp) = res

    for {
      keyPair1 <- KeyPairGenerator.makeKeyPair[IO]
      address1 = keyPair1.getPublic().toAddress
      output1 <- mkStateChannelOutput(keyPair1)
      keyPair2 <- KeyPairGenerator.makeKeyPair[IO]
      address2 = keyPair2.getPublic().toAddress
      output2 <- mkStateChannelOutput(keyPair2)
      snapshotInfo = mkGlobalSnapshotInfo()
      snapshot <- mkGlobalIncrementalSnapshot[IO](snapshotInfo)
      service <- mkProcessor(
        Map(address1 -> output1.snapshotBinary.proofs.map(_.id.toPeerId), address2 -> output2.snapshotBinary.proofs.map(_.id.toPeerId)),
        Some(address1 -> StateChannelValidator.NotSignedExclusivelyByStateChannelOwner)
      )
      expected = StateChannelAcceptanceResult(
        SortedMap((address2, NonEmptyList.of(output2.snapshotBinary))),
        SortedMap.empty[Address, StateChannelAcceptanceResult.CurrencySnapshotWithState],
        Set.empty,
        SortedMap.empty,
        SortedMap.empty[Address, List[StateChannelAcceptanceResult.CurrencySnapshotWithState]]
      )
      result <- service.process(
        SnapshotOrdinal(1L),
        snapshotInfo.balances,
        snapshotInfo.lastStateChannelSnapshotHashes,
        snapshotInfo.lastCurrencySnapshots,
        output1 :: output2 :: Nil,
        StateChannelValidationType.Full,
        _ => None.pure[IO]
      )
    } yield expect.eql(expected, result)

  }

  test("direct currency processing cannot bypass full state-channel validation") { res =>
    implicit val (ks, h, j, sp) = res

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      address = keyPair.getPublic.toAddress
      genesis = CurrencySnapshot.mkGenesis(Map.empty, None, None)
      signedGenesis <- forAsyncHasher(genesis, keyPair)
      content <- j.serialize(signedGenesis)
      binary <- forAsyncHasher(StateChannelSnapshotBinary(Hash.empty, content, SnapshotFee.MinValue), keyPair)
      events = SortedMap(address -> NonEmptyList.one(binary))
      validProcessor <- mkProcessor(Map(address -> binary.proofs.map(_.id.toPeerId)))
      rejectingProcessor <- mkProcessor(
        Map(address -> binary.proofs.map(_.id.toPeerId)),
        Some(address -> StateChannelValidator.BinaryFeeNotSufficient(SnapshotFee.MinValue, SnapshotFee(1L), 1, SnapshotOrdinal(1L)))
      )
      accepted <- validProcessor.processCurrencySnapshots(
        SnapshotOrdinal(1L),
        SortedMap.empty,
        SortedMap.empty,
        events,
        _ => none.pure[IO]
      )
      rejected <- rejectingProcessor.processCurrencySnapshots(
        SnapshotOrdinal(1L),
        SortedMap.empty,
        SortedMap.empty,
        events,
        _ => none.pure[IO]
      )
    } yield expect(accepted.contains(address)).and(expect(rejected.isEmpty))
  }

  test("checkpoint replay marks a valid currency prefix followed by an undecodable child as incomplete") { res =>
    implicit val (ks, h, j, sp) = res
    import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.GlobalSnapshotStateChannelEventsProcessor.CurrencyWindowConsumption
    import io.constellationnetwork.security.signature.Signed.SignedOps

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      address = keyPair.getPublic.toAddress
      genesis = CurrencySnapshot.mkGenesis(Map.empty, None, None)
      signedGenesis <- forAsyncHasher(genesis, keyPair)
      genesisContent <- j.serialize(signedGenesis)
      genesisBinary <- forAsyncHasher(
        StateChannelSnapshotBinary(Hash.empty, genesisContent, SnapshotFee.MinValue),
        keyPair
      )
      genesisBinaryHash <- genesisBinary.toHashed[IO].map(_.hash)
      undecodableBinary <- forAsyncHasher(
        StateChannelSnapshotBinary(genesisBinaryHash, Array[Byte](0x01, 0x02, 0x03), SnapshotFee.MinValue),
        keyPair
      )
      processor <- mkProcessor(Map(address -> genesisBinary.proofs.map(_.id.toPeerId)))
      orderedWindow = NonEmptyList.of(genesisBinary, undecodableBinary)
      raw <- processor.processCurrencySnapshots(
        SnapshotOrdinal(1L),
        SortedMap.empty,
        SortedMap.empty,
        SortedMap(address -> orderedWindow.reverse),
        _ => none.pure[IO]
      )
      replay <- processor.processCurrencySnapshotsWithCompleteConsumption(
        SnapshotOrdinal(1L),
        SortedMap.empty,
        SortedMap.empty,
        SortedMap(address -> orderedWindow),
        _ => none.pure[IO]
      )
      returned = raw.get(address).map(_._1.toList.map(_._1))
    } yield
      expect.all(
        // The legacy transition function still exposes its valid prefix to ordinary callers.
        returned.exists(binaries => binaries.size == 1 && binaries.head.value.content.sameElements(genesisBinary.value.content)),
        // The checkpoint-specific boundary separately proves that the exact two-input signed window was not consumed.
        replay.consumption(address).contains(CurrencyWindowConsumption.Incomplete(expectedInputs = 2, processedInputs = 1)),
        replay.completeResult(address).isEmpty
      )
  }

  test("shared fee payer debits are serialized in canonical metagraph order") { res =>
    implicit val (ks, h, j, sp) = res
    implicit val currencySelector: CurrencyStateProofSelector = CurrencyStateProofSelector.instance

    val ordinal = SnapshotOrdinal.unsafeApply(1L)
    val fee = SnapshotFee(10L)
    val feeConfigs = SortedMap(
      SnapshotOrdinal.MinValue -> FeeCalculatorConfig(
        baseFee = 1L,
        stakingWeight = BigDecimal(0),
        computationalCost = 1L,
        proWeight = BigDecimal(0)
      )
    )

    val emptyInfo = CurrencySnapshotInfo(
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

    val messageContextFns = new CurrencySnapshotContextFunctions[IO] {
      def createContext(
        context: CurrencySnapshotContext,
        lastArtifact: Signed[CurrencyIncrementalSnapshot],
        signedArtifact: Signed[CurrencyIncrementalSnapshot],
        getGlobalSnapshotByOrdinal: SnapshotOrdinal => IO[Option[Hashed[GlobalIncrementalSnapshot]]]
      )(implicit hasher: Hasher[IO]): IO[CurrencySnapshotContext] = {
        val ownerMessage = signedArtifact.value.messages.flatMap(_.find(_.value.messageType === MessageType.Owner))
        val lastMessages = ownerMessage.map(message => SortedMap[MessageType, Signed[CurrencyMessage]](MessageType.Owner -> message))
        context.copy(snapshotInfo = context.snapshotInfo.copy(lastMessages = lastMessages)).pure[IO]
      }
    }

    val enforceUniqueFeeAddress
      : (StateChannelOutput, SnapshotFeesInfo) => StateChannelValidator.StateChannelValidationErrorOr[StateChannelOutput] =
      (output, feesInfo) =>
        StateChannelValidator
          .validateIfAddressAlreadyUsed(output.address, feesInfo.allFeesAddresses, feesInfo.ownerAddress)
          .as(output)

    def incrementalChain(
      metagraphKeyPair: KeyPair,
      ownerMessage: Signed[CurrencyMessage]
    ): IO[(Signed[CurrencyIncrementalSnapshot], Signed[StateChannelSnapshotBinary])] =
      for {
        firstValue <- CurrencyIncrementalSnapshot.fromCurrencySnapshot[IO](CurrencySnapshot.mkGenesis(Map.empty, None, None))(
          implicitly[Parallel[IO]],
          implicitly[Async[IO]],
          h,
          j,
          currencySelector
        )
        first <- forAsyncHasher(firstValue, metagraphKeyPair)
        firstHash <- first.toHashed.map(_.hash)
        secondValue = firstValue.copy(
          ordinal = SnapshotOrdinal.unsafeApply(1L),
          lastSnapshotHash = firstHash,
          messages = Some(SortedSet(ownerMessage))
        )
        second <- forAsyncHasher(secondValue, metagraphKeyPair)
        content <- j.serialize(second)
        binary <- forAsyncHasher(StateChannelSnapshotBinary(firstHash, content, fee), metagraphKeyPair)
      } yield (first, binary)

    for {
      feePayerKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      metagraphKeyPairA <- KeyPairGenerator.makeKeyPair[IO]
      metagraphKeyPairB <- KeyPairGenerator.makeKeyPair[IO]
      feePayer = feePayerKeyPair.getPublic.toAddress
      metagraphA = metagraphKeyPairA.getPublic.toAddress
      metagraphB = metagraphKeyPairB.getPublic.toAddress
      ownerMessageA <- forAsyncHasher(
        CurrencyMessage(MessageType.Owner, feePayer, metagraphA, MessageOrdinal.MinValue),
        feePayerKeyPair
      )
      ownerMessageB <- forAsyncHasher(
        CurrencyMessage(MessageType.Owner, feePayer, metagraphB, MessageOrdinal.MinValue),
        feePayerKeyPair
      )
      chainA <- incrementalChain(metagraphKeyPairA, ownerMessageA)
      chainB <- incrementalChain(metagraphKeyPairB, ownerMessageB)
      prior = SortedMap(
        metagraphA -> (Right((chainA._1, emptyInfo)): StateChannelAcceptanceResult.CurrencySnapshotWithState),
        metagraphB -> (Right((chainB._1, emptyInfo)): StateChannelAcceptanceResult.CurrencySnapshotWithState)
      )
      eventA = metagraphA -> NonEmptyList.one(chainA._2)
      eventB = metagraphB -> NonEmptyList.one(chainB._2)
      eventsAB = SortedMap.from(List(eventA, eventB))
      eventsBA = SortedMap.from(List(eventB, eventA))
      processor <- mkProcessorHarness(
        Map.empty,
        feeConfigs = feeConfigs,
        contextFnsOverride = messageContextFns.some
      ).map(_.processor)
      uniquenessProcessor <- mkProcessorHarness(
        Map.empty,
        feeConfigs = feeConfigs,
        contextFnsOverride = messageContextFns.some,
        validationOverride = enforceUniqueFeeAddress.some
      ).map(_.processor)
      currentBalances = SortedMap(feePayer -> Balance(100L))
      processedAB <- processor.processCurrencySnapshots(ordinal, currentBalances, prior, eventsAB, _ => none.pure[IO])
      processedBA <- processor.processCurrencySnapshots(ordinal, currentBalances, prior, eventsBA, _ => none.pure[IO])
      acceptedAB = processor.assembleAcceptanceResult(processedAB, prior, Set.empty)
      acceptedBA = processor.assembleAcceptanceResult(processedBA, prior, Set.empty)
      uniqueAB <- uniquenessProcessor.processCurrencySnapshots(ordinal, currentBalances, prior, eventsAB, _ => none.pure[IO])
      uniqueBA <- uniquenessProcessor.processCurrencySnapshots(ordinal, currentBalances, prior, eventsBA, _ => none.pure[IO])
      uniqueAcceptedAB = uniquenessProcessor.assembleAcceptanceResult(uniqueAB, prior, Set.empty)
      uniqueAcceptedBA = uniquenessProcessor.assembleAcceptanceResult(uniqueBA, prior, Set.empty)
    } yield
      expect
        .eql(Balance(80L).some, acceptedAB.balanceUpdate.get(feePayer))
        .and(expect.eql(acceptedAB.balanceUpdate, acceptedBA.balanceUpdate))
        .and(expect.eql(processedAB.keySet, processedBA.keySet))
        .and(expect.eql(Balance(90L).some, uniqueAcceptedAB.balanceUpdate.get(feePayer)))
        .and(expect.eql(uniqueAcceptedAB.balanceUpdate, uniqueAcceptedBA.balanceUpdate))
        .and(expect.eql(1, uniqueAB.size))
        .and(expect.eql(uniqueAB.keySet, uniqueBA.keySet))
  }

  test("an unseeded currency incremental is rejected instead of advancing as opaque state") { res =>
    implicit val (ks, h, j, sp) = res
    implicit val currencySelector: CurrencyStateProofSelector = CurrencyStateProofSelector.instance

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      address = keyPair.getPublic.toAddress
      genesis = CurrencySnapshot.mkGenesis(Map.empty, None, None)
      incremental <- CurrencyIncrementalSnapshot.fromCurrencySnapshot[IO](genesis)(
        implicitly[Parallel[IO]],
        implicitly[Async[IO]],
        h,
        j,
        currencySelector
      )
      signedIncremental <- forAsyncHasher(incremental, keyPair)
      content <- j.serialize(signedIncremental)
      binary <- forAsyncHasher(StateChannelSnapshotBinary(Hash.empty, content, SnapshotFee.MinValue), keyPair)
      processor <- mkProcessor(Map(address -> binary.proofs.map(_.id.toPeerId)))
      result <- processor.processCurrencySnapshots(
        SnapshotOrdinal(1L),
        SortedMap.empty,
        SortedMap.empty,
        SortedMap(address -> NonEmptyList.one(binary)),
        _ => none.pure[IO]
      )
    } yield expect(result.isEmpty)
  }

  def mkStateChannelOutput(keyPair: KeyPair, hash: Option[Hash] = None)(
    implicit S: SecurityProvider[IO],
    H: Hasher[IO],
    J: JsonSerializer[IO]
  ) = for {
    content <- Random.scalaUtilRandom[IO].flatMap(_.nextString(10))
    compressedBytes <- J.serialize(content)
    binary <- StateChannelSnapshotBinary(hash.getOrElse(Hash.empty), compressedBytes, SnapshotFee.MinValue).pure[IO]
    signedSC <- forAsyncHasher(binary, keyPair)
  } yield StateChannelOutput(keyPair.getPublic.toAddress, signedSC)

  def mkGlobalIncrementalSnapshot[F[_]: Parallel: Async: Hasher: JsonSerializer](
    globalSnapshotInfo: GlobalSnapshotInfo
  ): F[Hashed[GlobalIncrementalSnapshot]] =
    globalSnapshotInfo.stateProof[F](SnapshotOrdinal(NonNegLong(1L))).flatMap { sp =>
      Signed(
        GlobalIncrementalSnapshot(
          SnapshotOrdinal(NonNegLong(1L)),
          Height.MinValue,
          SubHeight.MinValue,
          Hash.empty,
          SortedSet.empty,
          SortedMap.empty,
          SortedMap.empty[io.constellationnetwork.schema.sharding.ShardId, io.constellationnetwork.schema.sharding.ShardCheckpoint],
          SortedSet.empty,
          None,
          EpochProgress.MinValue,
          NonEmptyList.of(PeerId(Hex(""))),
          SnapshotTips(SortedSet.empty, SortedSet.empty),
          stateProof = sp,
          Some(SortedSet.empty),
          Some(SortedSet.empty),
          Some(SortedMap.empty),
          Some(SortedMap.empty),
          Some(SortedSet.empty),
          Some(SortedMap.empty),
          Some(SortedMap.empty),
          Some(SortedMap.empty),
          Some(SortedMap.empty)
        ),
        NonEmptySet.fromSetUnsafe(SortedSet(SignatureProof(ID.Id(Hex("")), Signature(Hex("")))))
      ).toHashed[F]
    }

  def mkGlobalSnapshotInfo(lastStateChannelSnapshotHashes: SortedMap[Address, Hash] = SortedMap.empty) =
    GlobalSnapshotInfo(
      lastStateChannelSnapshotHashes,
      SortedMap.empty,
      SortedMap.empty,
      SortedMap.empty,
      SortedMap.empty,
      None,
      None,
      None,
      None,
      None,
      Some(SortedMap.empty),
      Some(SortedMap.empty),
      Some(SortedMap.empty),
      Some(SortedMap.empty),
      Some(SortedMap.empty),
      Some(SortedMap.empty),
      Some(SortedMap.empty),
      SortedMap.empty
    )

}
