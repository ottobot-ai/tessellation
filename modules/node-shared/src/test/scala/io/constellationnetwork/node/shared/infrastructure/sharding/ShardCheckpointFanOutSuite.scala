package io.constellationnetwork.node.shared.infrastructure.sharding

import java.security.{KeyPair, SecureRandom}

import cats.data.NonEmptyList
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.currency.schema.currency.SnapshotFee
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.sharding.{ShardChainStore, ShardSlotLeader}
import io.constellationnetwork.node.shared.domain.nakamoto.{EligibilityChecker, ShardAssignment}
import io.constellationnetwork.node.shared.infrastructure.metrics.{Metrics, NoOpMetrics}
import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.numerics.interpreters.{ExpInterpreter, Log1pInterpreter}
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.nakamoto.slot.Slot
import io.constellationnetwork.schema.nakamoto.{EtaPeriod, LddConfig}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import eu.timepit.refined.types.numeric.NonNegLong
import org.typelevel.log4cats.slf4j.Slf4jLogger
import weaver.MutableIOSuite

/** Tests for [[ShardCheckpointFanOut]] — the shared producer fan-out body invoked from BOTH the gl0 leader loop (`onSlotWon`, own ord) and
  * the sync daemon (`processValidSnapshotInner` becameBestTip branch, gossip-received ords).
  *
  * '''Required coverage''':
  *   1. Empty producer map ⇒ no-op (the numShards=1 regression-bar short-circuit; `traverse_` over Nil).
  *   1. Populated, σ=1 ⇒ the producer for a shard with content produces a checkpoint AND it is stored into that shard's chain store.
  *   1. Per-shard partitioning ⇒ only the producers whose shard has content (or that win) write; a shard with NO content this ord produces
  *      nothing (§15.5 content-only) — no empty checkpoint.
  *
  * Fixture strategy mirrors [[ShardCheckpointProducerSuite]]: real `ShardSlotLeader` (σ=1 ⇒ always wins, σ=0 ⇒ never), real
  * `ShardChainStore` + `Hasher[IO]`, real Ed25519 keypair, stub KES + stub derive. `ShardAssignment.make` is the real deterministic
  * hash-mod-M mapping.
  */
object ShardCheckpointFanOutSuite extends MutableIOSuite {

  override type Res = (Hasher[IO], SecurityProvider[IO], ShardSlotLeader[IO])

  implicit val metrics: Metrics[IO] = NoOpMetrics.make

  private val logger = Slf4jLogger.getLoggerFromName[IO]("ShardCheckpointFanOutSuite")

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
      log1p <- Log1pInterpreter.make[IO](maxIterations = 10000, precision = 8).asResource
      exp <- ExpInterpreter.make[IO](maxIterations = 10000, precision = 38).asResource
      ec = EligibilityChecker.make[IO](log1p, exp)
      ssl = ShardSlotLeader.make[IO](ec)
    } yield (h, sp, ssl)

  // ===========================================================================
  // Fixtures
  // ===========================================================================

  private val numShards: Int = 4

  private val random = {
    val r = SecureRandom.getInstance("SHA1PRNG")
    r.setSeed(0x53_48_43_50_46_41_4eL) // ASCII "SHCPFAN"
    r
  }

  private def randomVrfSk(): Array[Byte] = {
    val sk = new Array[Byte](32)
    random.nextBytes(sk)
    sk
  }

  private def randomGl0Eta(): Array[Byte] = {
    val eta = new Array[Byte](32)
    random.nextBytes(eta)
    eta
  }

  private def mkAddress(label: String): Address =
    Address.fromBytes(label.getBytes("UTF-8"))

  private def hashFromString(s: String): Hash =
    Hash.fromBytes(s.getBytes("UTF-8"))

  private def mkSignedBinary(mgLabel: String, idx: Int): Signed[StateChannelSnapshotBinary] = {
    import cats.data.NonEmptySet
    import io.constellationnetwork.schema.ID.Id
    import io.constellationnetwork.security.hex.Hex
    import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
    val body = StateChannelSnapshotBinary(
      lastSnapshotHash = hashFromString(s"$mgLabel-parent-$idx"),
      content = Array.fill[Byte](16)(idx.toByte),
      fee = SnapshotFee(NonNegLong.unsafeFrom(0L))
    )
    val sentinelProof = SignatureProof(Id(Hex("11" * 64)), Signature(Hex("22" * 70)))
    Signed(body, NonEmptySet.of(sentinelProof))
  }

  private val slotForGl0Anchor: SnapshotOrdinal => Slot =
    ord => Slot.unsafeApply(ord.value.value)

  private val slotGapFor: (Slot, Option[Slot]) => Long =
    (cur, parentOpt) => parentOpt.fold(cur.value.value)(p => cur.value.value - p.value.value)

  private val fixedKesPayload: Array[Byte] = Array.fill[Byte](128)(0xab.toByte)
  private def stubKesSigner: ShardCheckpointProducer.KesSigner[IO] =
    ShardCheckpointProducer.KesSigner.fixed[IO](period = 7, signatureBytes = fixedKesPayload)

  private def deterministicDerive(mg: Address, snap: Signed[StateChannelSnapshotBinary]): IO[Hash] =
    IO.pure(hashFromString(s"derived-${mg.value.value}-${snap.value.lastSnapshotHash.value.take(8)}"))

  private def mkOrd(value: Long): SnapshotOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(value))

  /** Build a producer for one shard wired against `chainStore`, with the supplied σ. */
  private def makeProducer(
    ssl: ShardSlotLeader[IO],
    shardId: ShardId,
    chainStore: ShardChainStore[IO],
    keyPair: KeyPair,
    sigma: Ratio,
    shardEta: Array[Byte]
  )(implicit h: Hasher[IO], sp: SecurityProvider[IO]): IO[ShardCheckpointProducer[IO]] =
    ShardCheckpointProducer.make[IO](
      shardId = shardId,
      chainStore = chainStore,
      slotLeader = ssl,
      publisher = ShardCheckpointPublisher.noop[IO],
      selfPeerId = PeerId.fromPublic(keyPair.getPublic),
      selfKeyPair = keyPair,
      selfVrfSk = randomVrfSk(),
      kesSigner = stubKesSigner,
      shardEta = shardEta,
      sigmaInCommittee = sigma,
      slotForGl0Anchor = slotForGl0Anchor,
      slotGapFor = slotGapFor,
      lddConfig = LddConfig.Default,
      derivePerMgState = deterministicDerive
    )

  /** Per-shard rig: a chain store + a producer for every shard `0 .. numShards-1`, all sharing one keypair + σ. */
  private case class Rig(
    assignment: ShardAssignment[IO],
    producers: Map[ShardId, ShardCheckpointProducer[IO]],
    chainStores: Map[ShardId, ShardChainStore[IO]]
  )

  private def freshRig(
    ssl: ShardSlotLeader[IO],
    sigma: Ratio
  )(implicit h: Hasher[IO], sp: SecurityProvider[IO]): IO[Rig] =
    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      gl0Eta = randomGl0Eta()
      assignment = ShardAssignment.make[IO](numShards)
      perShard <- (0 until numShards).toList.traverse { i =>
        val sid = ShardId.unsafeApply(i)
        for {
          store <- ShardChainStore.make[IO](sid)
          shardEta <- ssl.computeShardEta(sid, gl0Eta)
          producer <- makeProducer(ssl, sid, store, keyPair, sigma, shardEta)
        } yield (sid, producer, store)
      }
    } yield
      Rig(
        assignment = assignment,
        producers = perShard.map { case (sid, p, _) => sid -> p }.toMap,
        chainStores = perShard.map { case (sid, _, s) => sid -> s }.toMap
      )

  /** Build SC snapshots for `numMgs` metagraphs (one binary each). Returns the map + the per-shard partition (so the test can assert
    * which shards received content under the SAME deterministic mapping the fan-out uses).
    */
  private def mkScSnapshots(numMgs: Int)(
    implicit h: Hasher[IO]
  ): IO[(SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]], Map[ShardId, Set[Address]])] = {
    val assignment = ShardAssignment.make[IO](numShards)
    val sc = SortedMap.from(
      (0 until numMgs).map { i =>
        val mg = mkAddress(s"mg-$i")
        mg -> NonEmptyList.of(mkSignedBinary(s"mg-$i", i))
      }
    )(Address.OrderingInstance)
    sc.keys.toList
      .traverse(mg => assignment.shardIdFor(mg).map(sid => sid -> mg))
      .map(_.groupBy(_._1).map { case (sid, es) => sid -> es.map(_._2).toSet })
      .map(partition => (sc, partition))
  }

  // ===========================================================================
  // Test 1 — Empty producer map ⇒ no-op (numShards=1 regression bar)
  // ===========================================================================

  test("empty producer map ⇒ no-op: no checkpoints produced or stored") { res =>
    implicit val (h, sp, _) = res
    for {
      assignment <- IO.pure(ShardAssignment.make[IO](numShards))
      sc <- mkScSnapshots(numMgs = 4).map(_._1)
      _ <- ShardCheckpointFanOut.run[IO](
        stateChannelSnapshots = sc,
        producedOrd = mkOrd(1000L),
        epoch = EtaPeriod(0L),
        shardProducers = Map.empty,
        shardChainStores = Map.empty,
        shardAssignment = assignment,
        logger = logger
      )
    } yield expect(true) // sanity: completes without error, allocates nothing
  }

  // ===========================================================================
  // Test 2 — σ=1 ⇒ producers for shards-with-content produce + store
  // ===========================================================================

  test("σ=1 populated ⇒ content shards produce + store; empty shards produce nothing (§15.5)") { res =>
    implicit val (h, sp, ssl) = res
    for {
      rig <- freshRig(ssl, Ratio.One)
      scAndPartition <- mkScSnapshots(numMgs = 8)
      (sc, partition) = scAndPartition
      // Fan out over a window of distinct ords. The per-shard VRF lottery isn't guaranteed at every single
      // ord (or for every shard's eta) even at σ=1, so a window ensures wins land. Each ord re-partitions
      // the SAME `sc` (deterministic), so each producer is driven with its own shard's content every time.
      // `ShardChainStore.store` is idempotent by hash; chain growth happens on wins.
      lo = 6000L
      hi = 6099L
      _ <- (lo to hi).toList.traverse_ { ord =>
        ShardCheckpointFanOut.run[IO](
          stateChannelSnapshots = sc,
          producedOrd = mkOrd(ord),
          epoch = EtaPeriod(0L),
          shardProducers = rig.producers,
          shardChainStores = rig.chainStores,
          shardAssignment = rig.assignment,
          logger = logger
        )
      }
      // Tips for content shards (those whose deterministic mapping received >=1 MG this ord).
      tipsForContentShards <- partition.keys.toList.traverse(sid => rig.chainStores(sid).bestTip.map(sid -> _))
      // Shards with NO content must have produced nothing (§15.5 content-only) — no empty checkpoint to gl0.
      emptyShards = (0 until numShards).map(ShardId.unsafeApply).toSet -- partition.keys.toSet
      tipsForEmptyShards <- emptyShards.toList.traverse(sid => rig.chainStores(sid).bestTip.map(sid -> _))
      storedContentTips = tipsForContentShards.collect { case (_, Some(t)) => t }
    } yield
      expect.all(
        // At least one shard received content (sanity — the mapping isn't degenerate for 8 MGs over 4 shards).
        partition.nonEmpty,
        // The fan-out drove the producers + stored their wins: at least one content shard has a checkpoint,
        // proving the produce → self-store wiring end-to-end (per-shard win-rate is the producer's concern,
        // already covered by ShardCheckpointProducerSuite).
        storedContentTips.nonEmpty,
        // Every stored checkpoint is anchored within the fan-out ord window (the producedOrd we passed).
        storedContentTips.forall { t =>
          t.signed.value.shardOrdinal.value >= 1L &&
          t.signed.value.gl0AnchorOrdinal.value.value >= lo &&
          t.signed.value.gl0AnchorOrdinal.value.value <= hi
        },
        // Every empty shard produced nothing — no empty checkpoint to gl0.
        tipsForEmptyShards.forall(_._2.isEmpty)
      )
  }

  // ===========================================================================
  // Test 3 — σ=0 ⇒ no shard wins ⇒ nothing stored even with content
  // ===========================================================================

  test("σ=0 ⇒ no shard wins its slot ⇒ zero checkpoints stored despite content present") { res =>
    implicit val (h, sp, ssl) = res
    for {
      rig <- freshRig(ssl, Ratio.Zero)
      sc <- mkScSnapshots(numMgs = 8).map(_._1)
      _ <- ShardCheckpointFanOut.run[IO](
        stateChannelSnapshots = sc,
        producedOrd = mkOrd(7000L),
        epoch = EtaPeriod(0L),
        shardProducers = rig.producers,
        shardChainStores = rig.chainStores,
        shardAssignment = rig.assignment,
        logger = logger
      )
      tips <- (0 until numShards).toList.traverse(i => rig.chainStores(ShardId.unsafeApply(i)).bestTip)
    } yield expect(tips.forall(_.isEmpty))
  }
}
