package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import java.security.KeyPair

import cats.data.NonEmptyList
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.currency.schema.currency.{CurrencySnapshot, SnapshotFee}
import io.constellationnetwork.dag.l0.infrastructure.snapshot.GlobalSnapshotStateChannelEventsProcessorSuite
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.node.shared.domain.nakamoto.sharding.{ShardChainStore, ShardSlotLeader}
import io.constellationnetwork.node.shared.domain.nakamoto.{CanonicalOperatorConsensusFixture, EligibilityChecker}
import io.constellationnetwork.node.shared.infrastructure.metrics.{Metrics, NoOpMetrics}
import io.constellationnetwork.node.shared.infrastructure.sharding.{
  ShardCheckpointProducer,
  ShardCheckpointPublisher,
  ShardCheckpointWiring
}
import io.constellationnetwork.numerics.interpreters.{ExpInterpreter, Log1pInterpreter}
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nakamoto.slot.Slot
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding.{ShardCheckpoint, ShardId}
import io.constellationnetwork.schema.{GlobalStateProofSelector, SnapshotOrdinal, StateProofSelector}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.kes.OperationalKeyMaker
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.Signed.forAsyncHasher
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}
import io.constellationnetwork.shared.sharedKryoRegistrar
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** Golden producer/verifier parity over the framework currency path.
  *
  * The producer and verifier use independently constructed real state-channel processors and MPT readers. The only common inputs are the
  * signed currency binary and checkpoint coordinates. A stubbed derivation cannot satisfy this test.
  */
object ShardCommitteeReExecutionSuite extends MutableIOSuite {

  override type Res = (KryoSerializer[IO], Hasher[IO], JsonSerializer[IO], SecurityProvider[IO], ShardSlotLeader[IO])

  implicit val metrics: Metrics[IO] = NoOpMetrics.make
  implicit val stateProofSelector: StateProofSelector =
    GlobalStateProofSelector(SnapshotOrdinal(NonNegLong.unsafeFrom(0L)))

  override def sharedResource: Resource[IO, Res] =
    for {
      implicit0(ks: KryoSerializer[IO]) <- KryoSerializer.forAsync[IO](sharedKryoRegistrar)
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
      log1p <- Log1pInterpreter.make[IO](maxIterations = 10000, precision = 8).asResource
      exp <- ExpInterpreter.make[IO](maxIterations = 10000, precision = 38).asResource
      slotLeader = ShardSlotLeader.make[IO](EligibilityChecker.make[IO](log1p, exp))
    } yield (ks, h, j, sp, slotLeader)

  private val shardZero: ShardId = ShardId.unsafeApply(0)
  private val epochZero: EtaPeriod = EtaPeriod(0L)
  private val anchorStart: Long = 1000L
  private val fixedShardEta: Array[Byte] = Array.fill[Byte](32)(0x7a.toByte)

  private def mkCurrencyGenesisBinary(
    metagraphKey: KeyPair
  )(implicit h: Hasher[IO], sp: SecurityProvider[IO], js: JsonSerializer[IO]): IO[Signed[StateChannelSnapshotBinary]] =
    for {
      signedGenesis <- forAsyncHasher[IO, CurrencySnapshot](CurrencySnapshot.mkGenesis(Map.empty, None, None), metagraphKey)
      content <- JsonSerializer[IO].serialize(signedGenesis)
      binary <- forAsyncHasher[IO, StateChannelSnapshotBinary](
        StateChannelSnapshotBinary(Hash.empty, content, SnapshotFee.MinValue),
        metagraphKey
      )
    } yield binary

  private def emptyReader(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[GlobalStateReader[IO]] =
    for {
      producer <- InMemoryMerklePatriciaProducer.make[IO]()
      store <- MptStore.make[IO, GlobalStateKey](producer, GlobalStateKey.toHex[IO])
    } yield GlobalStateReader.fromMptStore(store)

  private def makeReplay(
    reader: GlobalStateReader[IO]
  )(
    implicit h: Hasher[IO],
    js: JsonSerializer[IO],
    ks: KryoSerializer[IO],
    sp: SecurityProvider[IO]
  ): IO[(Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal, SnapshotOrdinal) => IO[Option[Hash]]] =
    GlobalSnapshotStateChannelEventsProcessorSuite.mkProcessor(Map.empty).map { processor =>
      ShardCheckpointWiring.reExecDerivationAtPinnedBase[IO](processor, _ => reader.some.pure[IO], _ => none.pure[IO])
    }

  private def makeProducer(
    slotLeader: ShardSlotLeader[IO],
    chainStore: ShardChainStore[IO],
    operator: CanonicalOperatorConsensusFixture,
    replay: (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal, SnapshotOrdinal) => IO[Option[Hash]]
  )(implicit h: Hasher[IO], sp: SecurityProvider[IO]): IO[ShardCheckpointProducer[IO]] = {
    val selfPeerId = operator.resolvedPair.operatorPeerId
    val selfVrfVk = operator.resolvedPair.vrfPublicKey.toBytes
    val kesSigner = new ShardCheckpointProducer.KesSigner[IO] {
      def sign(
        operatorKeys: io.constellationnetwork.node.shared.domain.nakamoto.kes.OperatorConsensusKeys,
        checkpointEpoch: EtaPeriod,
        message: Array[Byte]
      ): IO[Option[ShardCheckpointProducer.KesSignature]] =
        operator.kesSigner
          .signAt(Math.toIntExact(checkpointEpoch.value), message)
          .map(
            _.toOption.map(signature =>
              ShardCheckpointProducer.KesSignature(
                Math.toIntExact(checkpointEpoch.value),
                OperationalKeyMaker.encodeSignature(signature)
              )
            )
          )
    }

    ShardCheckpointProducer.make[IO](
      shardId = shardZero,
      chainStore = chainStore,
      executionBaseF = ShardCheckpointProducer.PinnedExecutionBase(SnapshotOrdinal.MinValue, SortedMap.empty[Address, Hash]).some.pure[IO],
      executionBaseAt =
        _ => ShardCheckpointProducer.PinnedExecutionBase(SnapshotOrdinal.MinValue, SortedMap.empty[Address, Hash]).some.pure[IO],
      adoptedPerMgTip = SortedMap.empty[Address, Hash].pure[IO],
      slotLeader = slotLeader,
      publisher = ShardCheckpointPublisher.noop[IO],
      selfPeerId = selfPeerId,
      selfKeyPair = operator.localLongTermKeyPairForConsensusTest,
      selfVrfSk = operator.localVrfSecret,
      selfVrfVk = selfVrfVk,
      operatorKeyRegistry = operator.operatorKeyRegistry,
      kesSigner = kesSigner,
      shardEtaFor = _ => fixedShardEta.pure[IO],
      staircaseDeltaSlots = 5,
      derivePerMgState = replay,
      lastPhase2Checkpoint = none.pure[IO],
      republishEveryTicks = 1
    )
  }

  private def produceUntilSome(
    producer: ShardCheckpointProducer[IO],
    pending: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
    committee: Set[PeerId],
    attempt: Int = 0
  ): IO[Signed[ShardCheckpoint]] =
    if (attempt >= 100) IO.raiseError(new RuntimeException("single-member producer did not acquire duty within 100 slots"))
    else {
      val ordinal = SnapshotOrdinal(NonNegLong.unsafeFrom(anchorStart + attempt.toLong))
      val slot = Slot.unsafeApply(anchorStart + attempt.toLong)
      producer.produce(pending, ordinal, epochZero, slot, committee).flatMap {
        case Some(checkpoint) => checkpoint.pure[IO]
        case None             => produceUntilSome(producer, pending, committee, attempt + 1)
      }
    }

  test("real currency binary: producer root equals an independent verifier replay root") { res =>
    implicit val (ks, h, js, sp, slotLeader) = res
    CanonicalOperatorConsensusFixture.make.use { operator =>
      for {
        metagraphKey <- KeyPairGenerator.makeKeyPair[IO]
        metagraph = PublicKeyOps(metagraphKey.getPublic).toAddress
        binary <- mkCurrencyGenesisBinary(metagraphKey)
        pending = SortedMap(metagraph -> NonEmptyList.one(binary))(Address.OrderingInstance)
        producerReader <- emptyReader
        verifierReader <- emptyReader
        producerReplay <- makeReplay(producerReader)
        verifierReplay <- makeReplay(verifierReader)
        chainStore <- ShardChainStore.make[IO](shardZero)
        producer <- makeProducer(slotLeader, chainStore, operator, producerReplay)
        checkpoint <- produceUntilSome(producer, pending, Set(operator.resolvedPair.operatorPeerId))
        producedRoot = checkpoint.value.derivedStateDelta.perMetagraphMptRoots(metagraph)
        verifierRoot <- verifierReplay(
          metagraph,
          checkpoint.value.derivedStateDelta.includedSnapshots(metagraph),
          checkpoint.value.gl0AnchorOrdinal,
          checkpoint.value.executionBaseOrdinal
        )
        addressOnlySentinel <- Hasher[IO].hash(metagraph)
      } yield
        expect.all(
          producedRoot =!= Hash.empty,
          verifierRoot.contains(producedRoot),
          producedRoot =!= addressOnlySentinel,
          checkpoint.value.derivedStateDelta.includedSnapshots(metagraph).size == 1
        )
    }
  }
}
