package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.effect.{IO, Ref, Resource}
import cats.syntax.all._

import scala.collection.immutable.SortedSet

import io.constellationnetwork.currency.schema.currency._
import io.constellationnetwork.currency.schema.globalSnapshotSync.GlobalSyncView
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto._
import io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardBinaryBuffer
import io.constellationnetwork.node.shared.infrastructure.metrics.{Metrics, NoOpMetrics}
import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.{SnapshotOrdinal, SnapshotTips}
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.statechannel.{StateChannelOutput, StateChannelSnapshotBinary}

import eu.timepit.refined.auto._
import org.typelevel.log4cats.slf4j.Slf4jLogger
import weaver.MutableIOSuite

/** Regression for the #213/#290 metagraph-admission lag fix in [[NakamotoSyncDaemon.makeMetagraphBinaryProcessor]].
  *
  * '''The bug.''' `resolveContext` resolves an incoming binary's metagraph parent ordinal via two tiers: (1) the orphan buffer's
  * value-hash→ordinal admission cache (`lookupAdmittedOrd`), then (2) the tip-guarded signed currency-context resolver, which returns
  * `None` (logging "parentHash mismatch ... returning None") whenever the binary's parent ≠ gl0's single recorded GSI tip. The admission
  * cache was seeded ONLY inside the `if (admitted)` branch — i.e. only when THIS node's local committee gate reached quorum. When the local
  * gate TIMES OUT (some peers were transiently behind and buffered the binary instead of attesting, so kQuorum wasn't reached locally) the
  * binary's value-hash→ordinal was never cached, even though its place in the chain is fixed and the cluster admits it via 2/3-attestation
  * / depth-k. The very next child then misses the cache, falls through to the tip-guarded resolver, finds gl0's GSI tip still trailing the
  * not-yet-finalized parent → mismatch → orphan-buffer; every successor re-buffers off it (the observed 733-mismatch / orphan re-buffer
  * loop, gl0 trailing ml0).
  *
  * '''The fix (under test).''' Seed the admission cache `(mg, valueHash(B)) → ord(B)` the instant the binary RESOLVES (its parent already
  * passed the tip / cached-ancestor identity guard), BEFORE — and independent of — the local gate outcome. `ord(B) = parentOrdinal + 1` is
  * a pure function of B's content, so every honest node caches the byte-identical value (split-safe). The child then resolves via the cache
  * and enters `attestAndAdmit` regardless of this node's gate result on the parent.
  *
  * '''Harness.''' A stub `MetagraphCommitteeGate` whose `attestAndAdmit` always returns `false` models the local gate timing out on every
  * binary. The stub context resolver models the tip-identity guard: it resolves only when the binary's parent is in a fixed known-tip set
  * (seeded with the genesis tip and NEVER advanced — exactly gl0's GSI lag), otherwise `None`. So a child whose parent is its predecessor's
  * value-hash can ONLY resolve through the admission cache. Pre-fix, the child would orphan-buffer; post-fix it resolves.
  */
object MetagraphBinaryProcessorAdmissionLagSuite extends MutableIOSuite {

  // makeMetagraphBinaryProcessor now carries a `Metrics` context bound (orphan-buffer occupancy +
  // metagraph→shard gauges). The admission-lag logic under test is metrics-agnostic, so a no-op sink suffices.
  implicit val metrics: Metrics[IO] = NoOpMetrics.make

  override type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO], HasherSelector[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
      hs = HasherSelector.forSyncAlwaysCurrent(h)
    } yield (h, sp, j, hs)

  private val logger = Slf4jLogger.getLoggerFromName[IO]("MetagraphBinaryProcessorAdmissionLagSuite")

  private val mgAddr: Address = Address("DAG0y4eLqhhXUafeE3mgBstezPTnr8L3tZjAtMWB")
  private val genesisTip: Hash = Hash("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

  /** Build a `Signed[CurrencyIncrementalSnapshot]` at `ordinal` — field shape mirrors `MetagraphParentOrdinalResolverSuite`. */
  private def mkIncremental(
    ordinal: Long
  )(implicit sp: SecurityProvider[IO], h: Hasher[IO]): IO[Signed[CurrencyIncrementalSnapshot]] = {
    val snapshot = CurrencyIncrementalSnapshot(
      SnapshotOrdinal.unsafeApply(ordinal),
      Height.MinValue,
      SubHeight.MinValue,
      Hash.empty,
      SortedSet.empty,
      SortedSet.empty,
      SnapshotTips(SortedSet.empty, SortedSet.empty),
      CurrencySnapshotStateProof(Hash.empty, Hash.empty, None, None, None, None, None, None, None),
      EpochProgress.MinValue,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      Some(GlobalSyncView(SnapshotOrdinal.unsafeApply(100L), Hash("b" * 64), EpochProgress.MinValue))
    )
    KeyPairGenerator.makeKeyPair[IO].flatMap(kp => Signed.forAsyncHasher[IO, CurrencyIncrementalSnapshot](snapshot, kp))
  }

  /** Build the wire bytes of a `Signed[StateChannelSnapshotBinary]` whose `content` decodes to an incremental currency snapshot at
    * `ordinal` and whose `lastSnapshotHash` (the chain-link parent) is `parent`. These are exactly the bytes the daemon's
    * `processBytes(address, bytes)` deserializes.
    */
  private def mkBinaryBytes(ordinal: Long, parent: Hash)(
    implicit sp: SecurityProvider[IO],
    h: Hasher[IO],
    json: JsonSerializer[IO]
  ): IO[Array[Byte]] =
    for {
      inc <- mkIncremental(ordinal)
      content <- JsonSerializer[IO].serialize(inc)
      binary = StateChannelSnapshotBinary(parent, content, SnapshotFee.MinValue)
      signedBinary <- KeyPairGenerator.makeKeyPair[IO].flatMap(kp => Signed.forAsyncHasher[IO, StateChannelSnapshotBinary](binary, kp))
      bytes <- JsonSerializer[IO].serialize(signedBinary)
    } yield bytes

  /** The value-hash a binary's child references — `Signed[StateChannelSnapshotBinary].toHashed.hash` = `signed.value.hash` — the same
    * identity gl0 writes into `lastStateChannelSnapshotHashes` and the daemon records into the admission cache.
    */
  private def valueHashOf(bytes: Array[Byte])(implicit sp: SecurityProvider[IO], h: Hasher[IO], json: JsonSerializer[IO]): IO[Hash] =
    JsonSerializer[IO].deserialize[Signed[StateChannelSnapshotBinary]](bytes).flatMap {
      case Right(signed) => signed.toHashed.map(_.hash)
      case Left(e)       => IO.raiseError(e)
    }

  /** Stub gate that always times out (`attestAndAdmit` → false) and records every `(parentHash)` it was asked to admit, so a test can prove
    * the processor reached the gate for a given binary (i.e. resolved it).
    */
  private def timeoutGate(attemptedRef: Ref[IO, List[Hash]]): MetagraphCommitteeGate[IO] =
    new MetagraphCommitteeGate[IO] {
      def attestAndAdmit(
        metagraphAddress: Address,
        parentHash: Hash,
        binaryHash: Hash,
        eta: Array[Byte],
        artifactPeriod: EtaPeriod,
        sigmaOperatorKey: Ratio
      ): IO[Boolean] = attemptedRef.update(parentHash :: _).as(false)
      def recordReceivedAttestation(
        att: MetagraphCommitteeGate.IncomingAttestation,
        eta: Array[Byte],
        artifactPeriod: EtaPeriod,
        lookupSenderStake: PeerId => IO[Ratio]
      ): IO[Unit] = IO.unit
      def pruneParents(metagraphAddress: Address, parents: Set[Hash]): IO[Unit] = IO.unit
    }

  private def admitGate: MetagraphCommitteeGate[IO] =
    new MetagraphCommitteeGate[IO] {
      def attestAndAdmit(
        metagraphAddress: Address,
        parentHash: Hash,
        binaryHash: Hash,
        eta: Array[Byte],
        artifactPeriod: EtaPeriod,
        sigmaOperatorKey: Ratio
      ): IO[Boolean] = IO.pure(true)
      def recordReceivedAttestation(
        att: MetagraphCommitteeGate.IncomingAttestation,
        eta: Array[Byte],
        artifactPeriod: EtaPeriod,
        lookupSenderStake: PeerId => IO[Ratio]
      ): IO[Unit] = IO.unit
      def pruneParents(metagraphAddress: Address, parents: Set[Hash]): IO[Unit] = IO.unit
    }

  /** A context resolver that models the tip-identity guard: resolves the signed ML0 continuity + GL0 anchor ONLY when `parent` is a known
    * recorded tip; else `None`. The known-tip set is seeded with the genesis tip and NEVER advances — exactly gl0's GSI lag.
    */
  private def tipGuardedResolver(knownTips: Set[Hash])(
    implicit json: JsonSerializer[IO]
  ): (Address, Hash, Array[Byte]) => IO[Option[MetagraphParentOrdinalResolver.CurrencyBinaryContext]] =
    (_, parent, content) =>
      if (knownTips.contains(parent)) MetagraphParentOrdinalResolver.currencyContextFromContent[IO](content)
      else IO.pure(none[MetagraphParentOrdinalResolver.CurrencyBinaryContext])

  private def verifyPhase2(
    context: MetagraphParentOrdinalResolver.CurrencyBinaryContext
  ): IO[Option[MetagraphParentOrdinalResolver.Phase2CurrencyBinaryContext]] =
    MetagraphParentOrdinalResolver.Phase2CurrencyBinaryContext.verify[IO](context)((_, _) => IO.pure(true))

  test("admission cache is seeded at RESOLVE-time even when the local gate times out") { res =>
    implicit val (h, sp, json, hs) = res
    for {
      buf <- MetagraphOrphanBuffer.make[IO](logger)
      attempted <- Ref.of[IO, List[Hash]](Nil)
      gate = timeoutGate(attempted)
      process = NakamotoSyncDaemon.makeMetagraphBinaryProcessor[IO](
        processMetagraphBinary = (_: StateChannelOutput) => IO.unit,
        committeeGate = gate,
        currencyContextFor = tipGuardedResolver(Set(genesisTip)),
        currencyContextFromContent = MetagraphParentOrdinalResolver.currencyContextFromContent[IO],
        verifyPhase2CurrencyContext = verifyPhase2,
        etaForPhase2Anchor = _ => IO.pure(Array.fill(32)(0.toByte)),
        etaRotationSnapshots = 10L,
        selfStake = IO.pure(Ratio(1, 8)),
        senderStakeLookup = (_: PeerId) => IO.pure(Ratio(1, 8)),
        orphanBuffer = buf,
        logger = logger
      )
      // Binary B at ordinal 1, parent == the (known) genesis tip — resolves via the tip-guard.
      bBytes <- mkBinaryBytes(1L, genesisTip)
      bValueHash <- valueHashOf(bBytes)
      _ <- process(mgAddr, bBytes)
      // The gate was reached (B resolved) but returned false (timeout).
      reachedGate <- attempted.get.map(_.contains(genesisTip))
      // THE FIX: B's value-hash → its ordinal (1) is cached despite the gate timeout.
      cached <- buf.lookupAdmittedOrd(mgAddr, bValueHash)
      // B itself was NOT orphan-buffered (it resolved).
      bufferedAfterB <- buf.size
    } yield
      expect(reachedGate)
        .and(expect(cached.contains(1L)))
        .and(expect.eql(0, bufferedAfterB))
  }

  test("child of a gate-timed-out parent RESOLVES via the cache instead of re-buffering (the 733-mismatch loop)") { res =>
    implicit val (h, sp, json, hs) = res
    for {
      buf <- MetagraphOrphanBuffer.make[IO](logger)
      attempted <- Ref.of[IO, List[Hash]](Nil)
      gate = timeoutGate(attempted)
      // The tip-guard knows ONLY the genesis tip and never advances (gl0 GSI lag). So child C's parent
      // (== valueHash(B)) is NOT a known tip — C can only resolve through the admission cache the fix seeds.
      process = NakamotoSyncDaemon.makeMetagraphBinaryProcessor[IO](
        processMetagraphBinary = (_: StateChannelOutput) => IO.unit,
        committeeGate = gate,
        currencyContextFor = tipGuardedResolver(Set(genesisTip)),
        currencyContextFromContent = MetagraphParentOrdinalResolver.currencyContextFromContent[IO],
        verifyPhase2CurrencyContext = verifyPhase2,
        etaForPhase2Anchor = _ => IO.pure(Array.fill(32)(0.toByte)),
        etaRotationSnapshots = 10L,
        selfStake = IO.pure(Ratio(1, 8)),
        senderStakeLookup = (_: PeerId) => IO.pure(Ratio(1, 8)),
        orphanBuffer = buf,
        logger = logger
      )
      bBytes <- mkBinaryBytes(1L, genesisTip)
      bValueHash <- valueHashOf(bBytes)
      _ <- process(mgAddr, bBytes) // gate times out; fix caches (valueHash(B) -> 1)
      // Child C at ordinal 2, parent == valueHash(B).
      cBytes <- mkBinaryBytes(2L, bValueHash)
      _ <- attempted.set(Nil) // isolate C's gate attempt from B's
      _ <- process(mgAddr, cBytes)
      // C reached the gate keyed on its parent == valueHash(B): it RESOLVED (did not orphan-buffer).
      cReachedGate <- attempted.get.map(_.contains(bValueHash))
      // Nothing is orphan-buffered: neither B nor C re-buffered.
      bufferedAfter <- buf.size
      // C's own value-hash → ordinal 2 is now cached too — the cascade continues for a grandchild.
      cValueHash <- valueHashOf(cBytes)
      cCached <- buf.lookupAdmittedOrd(mgAddr, cValueHash)
    } yield
      expect(cReachedGate)
        .and(expect.eql(0, bufferedAfter))
        .and(expect(cCached.contains(2L)))
  }

  test("genuinely-unknown parent still fails closed → orphan-buffered (fail-closed default preserved)") { res =>
    implicit val (h, sp, json, hs) = res
    val unknownParent: Hash = Hash("cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")
    for {
      buf <- MetagraphOrphanBuffer.make[IO](logger)
      attempted <- Ref.of[IO, List[Hash]](Nil)
      gate = timeoutGate(attempted)
      process = NakamotoSyncDaemon.makeMetagraphBinaryProcessor[IO](
        processMetagraphBinary = (_: StateChannelOutput) => IO.unit,
        committeeGate = gate,
        currencyContextFor = tipGuardedResolver(Set(genesisTip)),
        currencyContextFromContent = MetagraphParentOrdinalResolver.currencyContextFromContent[IO],
        verifyPhase2CurrencyContext = verifyPhase2,
        etaForPhase2Anchor = _ => IO.pure(Array.fill(32)(0.toByte)),
        etaRotationSnapshots = 10L,
        selfStake = IO.pure(Ratio(1, 8)),
        senderStakeLookup = (_: PeerId) => IO.pure(Ratio(1, 8)),
        orphanBuffer = buf,
        logger = logger
      )
      // A binary whose parent is neither a known tip nor a cached admission → must orphan-buffer.
      orphanBytes <- mkBinaryBytes(5L, unknownParent)
      _ <- process(mgAddr, orphanBytes)
      reachedGate <- attempted.get.map(_.nonEmpty)
      buffered <- buf.size
    } yield expect(!reachedGate).and(expect.eql(1, buffered))
  }

  test("execution-shard buffer is written only after metagraph admission succeeds") { res =>
    implicit val (h, sp, json, hs) = res
    val assignment = ShardAssignment.make[IO](numShards = 2)
    for {
      sid <- assignment.shardIdFor(mgAddr)
      shardBuffer <- ShardBinaryBuffer.make[IO](sid, cap = 16)
      buffers = Map(sid -> shardBuffer)
      binaryBytes <- mkBinaryBytes(1L, genesisTip)

      timeoutOrphans <- MetagraphOrphanBuffer.make[IO](logger)
      attempted <- Ref.of[IO, List[Hash]](Nil)
      timeoutProcess = NakamotoSyncDaemon.makeMetagraphBinaryProcessor[IO](
        processMetagraphBinary = (_: StateChannelOutput) => IO.unit,
        committeeGate = timeoutGate(attempted),
        currencyContextFor = tipGuardedResolver(Set(genesisTip)),
        currencyContextFromContent = MetagraphParentOrdinalResolver.currencyContextFromContent[IO],
        verifyPhase2CurrencyContext = verifyPhase2,
        etaForPhase2Anchor = _ => IO.pure(Array.fill(32)(0.toByte)),
        etaRotationSnapshots = 10L,
        selfStake = IO.pure(Ratio(1, 8)),
        senderStakeLookup = (_: PeerId) => IO.pure(Ratio(1, 8)),
        orphanBuffer = timeoutOrphans,
        shardBinaryBuffers = buffers,
        shardAssignment = assignment.some,
        logger = logger
      )
      _ <- timeoutProcess(mgAddr, binaryBytes)
      afterTimeout <- shardBuffer.snapshotPending

      admittedOrphans <- MetagraphOrphanBuffer.make[IO](logger)
      admittedProcess = NakamotoSyncDaemon.makeMetagraphBinaryProcessor[IO](
        processMetagraphBinary = (_: StateChannelOutput) => IO.unit,
        committeeGate = admitGate,
        currencyContextFor = tipGuardedResolver(Set(genesisTip)),
        currencyContextFromContent = MetagraphParentOrdinalResolver.currencyContextFromContent[IO],
        verifyPhase2CurrencyContext = verifyPhase2,
        etaForPhase2Anchor = _ => IO.pure(Array.fill(32)(0.toByte)),
        etaRotationSnapshots = 10L,
        selfStake = IO.pure(Ratio(1, 8)),
        senderStakeLookup = (_: PeerId) => IO.pure(Ratio(1, 8)),
        orphanBuffer = admittedOrphans,
        shardBinaryBuffers = buffers,
        shardAssignment = assignment.some,
        logger = logger
      )
      _ <- admittedProcess(mgAddr, binaryBytes)
      afterAdmission <- shardBuffer.snapshotPending
    } yield expect(afterTimeout.isEmpty).and(expect(afterAdmission.get(mgAddr).exists(_.size == 1)))
  }
}
