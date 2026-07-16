package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.schema.currency.SnapshotFee
import io.constellationnetwork.dag.l0.infrastructure.snapshot.GlobalSnapshotStateChannelEventsProcessorSuite
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.CanonicalOperatorConsensusFixture
import io.constellationnetwork.node.shared.domain.nakamoto.kes.OperatorConsensusKeys
import io.constellationnetwork.node.shared.domain.nakamoto.sharding.{ShardChainStore, ShardSlotLeader}
import io.constellationnetwork.node.shared.infrastructure.metrics.{Metrics, NoOpMetrics}
import io.constellationnetwork.node.shared.infrastructure.sharding.{
  ShardCheckpointProducer,
  ShardCheckpointPublisher,
  ShardCheckpointWiring
}
import io.constellationnetwork.schema.ID.IdOps
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nakamoto.slot.Slot
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding.{ShardCheckpoint, ShardId, ShardOrdinal}
import io.constellationnetwork.schema.{GlobalStateProofSelector, SnapshotOrdinal, StateProofSelector}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.Signed.forAsyncHasher
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}
import io.constellationnetwork.shared.sharedKryoRegistrar
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** Executable regression for `ECO-OPAQUE-SHARD-001`.
  *
  * Standalone opaque state-channel bytes may be authenticated carriage, but they cannot produce a framework-currency root or acquire a
  * shard execution-validity signature. The fixture routes a genuinely signed, nondecodable binary through the production checkpoint
  * replay adapter and then injects that exact adapter into the production checkpoint producer.
  */
object OpaqueShardReplayAuthoritySuite extends MutableIOSuite {

  override type Res = (KryoSerializer[IO], Hasher[IO], JsonSerializer[IO], SecurityProvider[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      implicit0(kryo: KryoSerializer[IO]) <- KryoSerializer.forAsync[IO](sharedKryoRegistrar)
      securityProvider <- SecurityProvider.forAsync[IO]
      implicit0(json: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      hasher = Hasher.forJson[IO]
    } yield (kryo, hasher, json, securityProvider)

  implicit val metrics: Metrics[IO] = NoOpMetrics.make
  implicit val stateProofSelector: StateProofSelector =
    GlobalStateProofSelector(SnapshotOrdinal.MinValue)

  private val shardZero: ShardId = ShardId.unsafeApply(0)
  private val gl0AnchorOrdinal: SnapshotOrdinal = SnapshotOrdinal.unsafeApply(1L)
  private val executionBaseOrdinal: SnapshotOrdinal = SnapshotOrdinal.MinValue

  test("ECO-OPAQUE-SHARD-001: signed standalone opaque carriage derives no root and cannot be signed or published") { res =>
    implicit val (kryo, hasher, json, securityProvider) = res

    CanonicalOperatorConsensusFixture.make.use { operator =>
      for {
        metagraphKeyPair <- KeyPairGenerator.makeKeyPair[IO]
        metagraph = PublicKeyOps(metagraphKeyPair.getPublic).toAddress
        opaqueBinary <- forAsyncHasher[IO, StateChannelSnapshotBinary](
          StateChannelSnapshotBinary(
            lastSnapshotHash = Hash.empty,
            content = Array[Byte](0x01, 0x02, 0x03),
            fee = SnapshotFee.MinValue
          ),
          metagraphKeyPair
        )
        windows = SortedMap(metagraph -> NonEmptyList.one(opaqueBinary))
        metagraphOperators = NonEmptySet.fromSetUnsafe(
          SortedSet.from(opaqueBinary.proofs.toNonEmptyList.toList.map(_.id.toPeerId))
        )
        harness <- GlobalSnapshotStateChannelEventsProcessorSuite.mkProcessorHarness(
          Map(metagraph -> metagraphOperators)
        )
        executionBase = ShardCheckpointProducer.PinnedExecutionBase(
          ordinal = executionBaseOrdinal,
          perMgTips = SortedMap.empty,
          priorCurrencySnapshots = SortedMap.empty,
          balances = SortedMap.empty
        )
        globalSnapshotLookup = (_: SnapshotOrdinal) => harness.initialGlobalSnapshot.some.pure[IO]
        realReplay = ShardCheckpointWiring.reExecDerivationsAtResolvedBase[IO](
          harness.processor,
          globalSnapshotLookup
        )
        replayedRoots <- realReplay(windows, gl0AnchorOrdinal, executionBase)

        chainStore <- ShardChainStore.make[IO](shardZero)
        replayCalls <- Ref.of[IO, Int](0)
        kesSignCalls <- Ref.of[IO, Int](0)
        publishCalls <- Ref.of[IO, Int](0)
        slotLeader = new ShardSlotLeader[IO] {
          def computeShardEta(shardId: ShardId, gl0Eta: Array[Byte])(implicit hasher: Hasher[IO]): IO[Array[Byte]] = {
            val _ = (shardId, gl0Eta, hasher)
            IO.pure(Array.fill[Byte](32)(0x11))
          }

          def dutyOrder(
            committee: List[PeerId],
            eta: Array[Byte],
            shardOrdinal: ShardOrdinal
          )(implicit hasher: Hasher[IO]): IO[List[PeerId]] = {
            val _ = (eta, shardOrdinal, hasher)
            IO.pure(committee)
          }

          def membershipProof(vrfSk: Array[Byte], eta: Array[Byte], slot: Slot): IO[Array[Byte]] = {
            val _ = (vrfSk, eta, slot)
            IO.pure(Array.fill[Byte](80)(0x22))
          }
        }
        kesSigner = new ShardCheckpointProducer.KesSigner[IO] {
          def sign(
            operatorKeys: OperatorConsensusKeys,
            checkpointEpoch: EtaPeriod,
            message: Array[Byte]
          ): IO[Option[ShardCheckpointProducer.KesSignature]] = {
            val _ = (operatorKeys, checkpointEpoch, message)
            kesSignCalls.update(_ + 1).as(ShardCheckpointProducer.KesSignature(0, Array[Byte](0x33)).some)
          }
        }
        publisher = new ShardCheckpointPublisher[IO] {
          def publish(checkpoint: Signed[ShardCheckpoint]): IO[Unit] = {
            val _ = checkpoint
            publishCalls.update(_ + 1)
          }
        }
        producer <- ShardCheckpointProducer.make[IO](
          shardId = shardZero,
          chainStore = chainStore,
          executionBaseF = executionBase.some.pure[IO],
          executionBaseAt = _ => executionBase.some.pure[IO],
          adoptedPerMgTip = SortedMap.empty[Address, Hash].pure[IO],
          slotLeader = slotLeader,
          publisher = publisher,
          selfPeerId = operator.resolvedPair.operatorPeerId,
          selfKeyPair = operator.localLongTermKeyPairForConsensusTest,
          selfVrfSk = operator.localVrfSecret,
          selfVrfVk = operator.resolvedPair.vrfPublicKey.toBytes,
          operatorKeyRegistry = operator.operatorKeyRegistry,
          kesSigner = kesSigner,
          shardEtaFor = _ => IO.pure(Array.fill[Byte](32)(0x44)),
          staircaseDeltaSlots = 5,
          derivePerMgState = (_, _, _, _) => none.pure[IO],
          derivePerMgStates = Some((batch, anchor, base) => replayCalls.update(_ + 1) >> realReplay(batch, anchor, base)),
          lastPhase2Checkpoint = none[(ShardOrdinal, Hash)].pure[IO],
          republishEveryTicks = 1
        )
        produced <- producer.produce(
          windows,
          gl0AnchorOrdinal,
          EtaPeriod.Zero,
          Slot(NonNegLong.unsafeFrom(1L)),
          Set(operator.resolvedPair.operatorPeerId)
        )
        replayCount <- replayCalls.get
        kesSignCount <- kesSignCalls.get
        publishCount <- publishCalls.get
      } yield
        expect.all(
          replayedRoots == SortedMap(metagraph -> Option.empty[Hash]),
          replayCount == 1,
          produced.isEmpty,
          kesSignCount == 0,
          publishCount == 0
        )
    }
  }
}
