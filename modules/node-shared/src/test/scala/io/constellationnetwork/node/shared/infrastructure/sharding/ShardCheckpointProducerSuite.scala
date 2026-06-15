package io.constellationnetwork.node.shared.infrastructure.sharding

import java.security.{KeyPair, SecureRandom}

import cats.data.NonEmptyList
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.currency.schema.currency.SnapshotFee
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.EligibilityChecker
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.ChangeSet
import io.constellationnetwork.node.shared.domain.nakamoto.sharding.{ShardChainStore, ShardSlotLeader}
import io.constellationnetwork.node.shared.infrastructure.metrics.{Metrics, NoOpMetrics}
import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.numerics.interpreters.{ExpInterpreter, Log1pInterpreter}
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.nakamoto.slot.Slot
import io.constellationnetwork.schema.nakamoto.slot.{Slot => SlotT}
import io.constellationnetwork.schema.nakamoto.{EtaPeriod, LddConfig}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.verifySignatureProof
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** Tests for [[ShardCheckpointProducer]] — slice 8 of `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §6.
  *
  * Required coverage (per slice 8 task spec):
  *   1. '''Happy path''': this node wins the slot VRF ⇒ produce returns `Some`, publisher recorded the checkpoint, checkpoint has correct
  *      shardId + shardOrdinal = parent + 1 + this node's `CommitteeMemberSignature`.
  *   1. '''Skip when not leader''': this node doesn't win the slot VRF ⇒ produce returns `None`, publisher not called.
  *   1. '''Empty pending snapshots''': with empty input map, produce returns `None` (nothing to checkpoint).
  *   1. '''Parent chain-link''': produced checkpoint's `parentCheckpointHash` matches `bestTip` of `ShardChainStore`.
  *   1. '''Shard ordinal monotonicity''': after producing one checkpoint, the next call produces ord = prior + 1.
  *   1. '''derivePerMgState invoked per MG''': callback called once per MG; produced delta's `perMetagraphMptRoots` has entries per MG.
  *   1. '''Signature verifies''': the attached `CommitteeMemberSignature`'s Ed25519 sig verifies under the test's Ed25519 VK.
  *
  * '''Fixture strategy''': mix real + stubbed primitives.
  *   - '''Real''' `ShardSlotLeader` (so the VRF lottery is exercised end-to-end) with a controllable σ in committee (σ=1 → always wins, σ=0
  *     → never wins) — the same trick `ShardSlotLeaderSuite` uses for its corner cases. This avoids mocking the slot leader behaviour and
  *     keeps the producer path real.
  *   - '''Real''' `ShardChainStore` + `Hasher[F]` so the canonical preimage hash is byte-equivalent to runtime.
  *   - '''Real''' Ed25519 key pair (via `KeyPairGenerator.makeKeyPair`) so the signature-verification test is end-to-end.
  *   - '''Stubbed''' `KesSigner` (the fixture passes a constant byte payload — slice 8 doesn't verify KES; that's slice 14's gossip-side
  *     verifier) and stubbed `derivePerMgState` (returns a deterministic per-MG hash from a seed).
  */
object ShardCheckpointProducerSuite extends MutableIOSuite {

  override type Res = (Hasher[IO], SecurityProvider[IO], ShardSlotLeader[IO])

  // Slice 19: ShardChainStore.make requires Metrics[F]. No-op interpreter keeps the producer tests focused on
  // chain-link + signature semantics.
  implicit val metrics: Metrics[IO] = NoOpMetrics.make

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

  private val shardZero: ShardId = ShardId.unsafeApply(0)

  // Deterministic SHA1PRNG keyed by an ASCII tag — matches the seeding pattern in ShardSlotLeaderSuite so test draws are reproducible.
  private val random = {
    val r = SecureRandom.getInstance("SHA1PRNG")
    r.setSeed(0x53_48_43_50_52_4f_44L) // ASCII "SHCPROD"
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

  /** Build an Address from a label string — same shape as the slashing-suite fixtures. */
  private def mkAddress(label: String): Address =
    Address.fromBytes(label.getBytes("UTF-8"))

  /** Hash of a byte sequence — used to build per-MG SC binary stubs without standing up the currency stack. */
  private def hashFromString(s: String): Hash =
    Hash.fromBytes(s.getBytes("UTF-8"))

  /** Build a stub `Signed[StateChannelSnapshotBinary]` anchored on `parent`.
    *
    * EXECUTION-SHARDING R-2: `produce` now chain-link-orders pending binaries off the shard's `perMgTip` (genesis ⇒ `Hash.empty`). So the
    * binary's `lastSnapshotHash` MUST equal the anchor it is meant to chain off — `Hash.empty` for the genesis round, or the prior round's
    * included-binary hash thereafter. `content` varies by `(mgLabel, idx)` so distinct binaries get distinct canonical hashes.
    */
  private def mkSignedBinary(mgLabel: String, idx: Int, parent: Hash = Hash.empty): Signed[StateChannelSnapshotBinary] = {
    import cats.data.NonEmptySet
    import io.constellationnetwork.schema.ID.Id
    import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
    val _ = mgLabel
    val body = StateChannelSnapshotBinary(
      lastSnapshotHash = parent,
      content = Array.fill[Byte](16)(idx.toByte),
      fee = SnapshotFee(NonNegLong.unsafeFrom(0L))
    )
    val sentinelProof = SignatureProof(Id(Hex("11" * 64)), Signature(Hex("22" * 70)))
    Signed(body, NonEmptySet.of(sentinelProof))
  }

  /** Build pending snapshots map for `numMgs` MGs, with one signed binary each, all anchored at genesis (`Hash.empty`). The `SortedMap`
    * order is the same as the address ordering — the test asserts that `derivePerMgState` is called once per MG regardless of order.
    */
  private def mkPendingSnapshots(numMgs: Int): SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]] =
    SortedMap.from(
      (0 until numMgs).map { i =>
        val mg = mkAddress(s"mg-$i")
        mg -> NonEmptyList.of(mkSignedBinary(s"mg-$i", i))
      }
    )

  /** EXECUTION-SHARDING R-2: build the next-round pending set chained off the shard's current `perMgTip`. For each MG present in
    * `perMgTip`, the new binary references that tip (so it's admissible to the producer's `chainLinkOrder`); for MGs absent from `perMgTip`
    * (none, in the sequential-produce tests) it anchors at genesis. Keeps the producer's chain-link gate satisfied across successive
    * produces.
    */
  private def mkPendingChainedOff(
    perMgTip: SortedMap[Address, Hash],
    round: Int
  ): SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]] =
    SortedMap.from(
      (0 until 2).map { i =>
        val mg = mkAddress(s"mg-$i")
        val parent = perMgTip.getOrElse(mg, Hash.empty)
        mg -> NonEmptyList.of(mkSignedBinary(s"mg-$i", i + round * 100, parent))
      }
    )

  /** Deterministic `derivePerMgState` that returns a `Hash` derived from `(mg, headBinary.lastSnapshotHash)` paired with an empty
    * `ChangeSet` (step-6 signature). The diff content is exercised by the ChangeSet/wiring suites; here the tests only assert on the per-MG
    * hash values present in the produced `derivedStateDelta.perMetagraphMptRoots`, so an empty diff is sufficient.
    */
  private def deterministicDerive(
    mg: Address,
    snaps: NonEmptyList[Signed[StateChannelSnapshotBinary]],
    anchor: SnapshotOrdinal
  ): IO[Option[(Hash, ChangeSet)]] = {
    val _ = anchor
    IO.pure(Some((hashFromString(s"derived-${mg.value.value}-${snaps.head.value.lastSnapshotHash.value.take(8)}"), ChangeSet.empty)))
  }

  /** A `derivePerMgState` callback that records which MGs it was invoked for. The test uses this to assert callback invocation count and
    * argument coverage matches the input map.
    */
  private def recordingDerive(
    seen: cats.effect.kernel.Ref[IO, List[Address]]
  ): (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal) => IO[Option[(Hash, ChangeSet)]] =
    (mg, snaps, anchor) => seen.update(_ :+ mg) >> deterministicDerive(mg, snaps, anchor)

  // Simple slot mapping: 1 slot per gl0 ord. Matches the e2e default cadence in spirit (slot-cadence is per-shard config; for tests,
  // identity-ish keeps the slot value bounded so the LDD ramp parameters are predictable).
  private val slotForGl0Anchor: SnapshotOrdinal => Slot =
    ord => Slot.unsafeApply(ord.value.value)

  // slotGap: simple subtraction in slot-space; genesis treats the parent as slot=0 (caller-supplied default).
  private val slotGapFor: (Slot, Option[Slot]) => Long =
    (cur, parentOpt) => parentOpt.fold(cur.value.value)(p => cur.value.value - p.value.value)

  /** Stub KES signer carrying a fixed byte payload + fixed period. The producer copies the bytes verbatim onto
    * `CommitteeMemberSignature.kesProductSig` — slice 8 doesn't run a KES verifier (that's slice 14's gossip-side concern).
    */
  private val fixedKesPayload: Array[Byte] = Array.fill[Byte](128)(0xab.toByte)
  private def stubKesSigner: ShardCheckpointProducer.KesSigner[IO] =
    ShardCheckpointProducer.KesSigner.fixed[IO](period = 7, signatureBytes = fixedKesPayload)

  // Convenience: bundle the common deps the producer constructor needs.
  private case class TestRig(
    chainStore: ShardChainStore[IO],
    publisher: ShardCheckpointPublisher[IO],
    recorded: IO[List[Signed[ShardCheckpoint]]],
    keyPair: KeyPair,
    selfPeerId: PeerId
  )

  /** Build a fresh per-test rig (chain store + recording publisher + a fresh keypair). */
  private def freshRig(implicit h: Hasher[IO], sp: SecurityProvider[IO]): IO[TestRig] =
    for {
      store <- ShardChainStore.make[IO](shardZero)
      pubPair <- ShardCheckpointPublisher.recording[IO]
      kp <- KeyPairGenerator.makeKeyPair[IO]
    } yield TestRig(store, pubPair._1, pubPair._2, kp, PeerId.fromPublic(kp.getPublic))

  /** Build a producer wired with the common defaults: real ShardSlotLeader, real chain store, stub KES, stub derive. The two knobs exposed
    * to tests are `sigmaInCommittee` (σ=1 ⇒ always wins, σ=0 ⇒ never wins) and the optional `derivePerMgState` override.
    */
  private def makeProducer(
    ssl: ShardSlotLeader[IO],
    rig: TestRig,
    sigma: Ratio,
    shardEta: Array[Byte],
    derive: (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal) => IO[Option[(Hash, ChangeSet)]] =
      deterministicDerive,
    republishEveryTicks: Int = 1,
    // S2: the finalized-base per-MG window anchor. `None` ⇒ the shard's `perMgTip` (so the pre-S2 cases keep their perMgTip-anchored
    // behavior); the dedicated base-anchored case passes a DISTINCT (lower) tip to prove the window re-includes base->latest.
    finalizedBaseTip: Option[IO[SortedMap[Address, Hash]]] = None,
    // Newness-gate (S2-deadlock fix): gl0's ADOPT tip. `None` ⇒ == the window anchor, so the gate is a NO-OP (every existing test
    // sees identical behavior). The dedicated newness-gate case passes a DISTINCT (higher) adopt tip to exercise stale-re-include omit.
    adoptedTip: Option[IO[SortedMap[Address, Hash]]] = None
  )(implicit h: Hasher[IO], sp: SecurityProvider[IO]): IO[ShardCheckpointProducer[IO]] =
    ShardCheckpointProducer.make[IO](
      shardId = shardZero,
      chainStore = rig.chainStore,
      finalizedBasePerMgTip = finalizedBaseTip.getOrElse(rig.chainStore.perMgTip),
      adoptedPerMgTip = adoptedTip.getOrElse(finalizedBaseTip.getOrElse(rig.chainStore.perMgTip)),
      slotLeader = ssl,
      publisher = rig.publisher,
      selfPeerId = rig.selfPeerId,
      selfKeyPair = rig.keyPair,
      selfVrfSk = randomVrfSk(),
      kesSigner = stubKesSigner,
      // Slice S4: producer takes an epoch-keyed eta resolver. Tests pass a fixed precomputed shardEta regardless of
      // epoch — the producer/verifier-agreement property is exercised in ShardSlotLeaderSuite; here we only assert the
      // producer threads the resolved eta through its leader draw, so a constant is sufficient.
      shardEtaFor = _ => IO.pure(shardEta),
      slotGapFor = slotGapFor,
      staircaseDeltaSlots = 5,
      derivePerMgState = derive,
      lastAdoptedOrd = cats.effect.IO.pure(None),
      pipelineDepth = Int.MaxValue,
      republishEveryTicks = republishEveryTicks
    )

  // ===========================================================================
  // Test 1 — Happy path: produce + publish when slot leader
  // ===========================================================================

  test("happy path: σ=1 ⇒ produces a checkpoint, publishes it, attaches CommitteeMemberSignature with correct shardId + ordinal") { res =>
    implicit val (h, sp, ssl) = res
    for {
      rig <- freshRig
      gl0Eta = randomGl0Eta()
      shardEta <- ssl.computeShardEta(shardZero, gl0Eta)
      // σ=1 guarantees the slot lottery always fires (threshold = 1 - (1-fB)^1 = fB; even at fB=1/20 we get one win in ~20 slots, but we
      // saturate by also using slotGap > γ so difficulty plateaus at fB ⇒ we keep trying until success). At σ=1 the threshold reaches
      // its maximum for the gap regime, so we'll typically win on the first try.
      producer <- makeProducer(ssl, rig, Ratio.One, shardEta)
      // We may need to try several gl0 anchors until we find one where this node wins the lottery; σ=1 should hit on the first try in
      // recovery regime, but a defensive loop covers the rare miss.
      result <- tryProduceUntilSome(producer, startOrd = 1000L, EtaPeriod(0L), maxAttempts = 100, committee = Set(rig.selfPeerId))
      produced <- IO.fromOption(result)(new RuntimeException("happy path: σ=1 producer should win within 100 attempts"))
      recorded <- rig.recorded
    } yield
      expect.all(
        produced.value.shardId == shardZero,
        produced.value.shardOrdinal == ShardOrdinal(1L),
        produced.value.committeeSignatures.size == 1,
        produced.value.committeeSignatures.head.peerId == rig.selfPeerId,
        produced.value.committeeSignatures.head.kesTreeStep == 7,
        recorded.size == 1,
        recorded.head.value.shardOrdinal == ShardOrdinal(1L),
        recorded.head.value.shardId == shardZero,
        produced.value.parentCheckpointHash == Hash.empty // genesis
      )
  }

  // ===========================================================================
  // Test 2 — Skip when not leader
  // ===========================================================================

  test("skip when not on duty: committee excludes self ⇒ produce returns None, publisher not called") { res =>
    implicit val (h, sp, ssl) = res
    for {
      rig <- freshRig
      gl0Eta = randomGl0Eta()
      shardEta <- ssl.computeShardEta(shardZero, gl0Eta)
      // Staircase: the duty schedule is over the COMMITTEE; a committee that excludes self ⇒ self is never on duty.
      producer <- makeProducer(ssl, rig, Ratio.Zero, shardEta)
      stranger = PeerId(Hex("ab" * 64))
      // Try several gl0 anchors — must all return None.
      attempts <- (1L to 10L).toList.traverse(i =>
        producer.produce(mkPendingSnapshots(2), mkOrd(i), EtaPeriod(0L), Slot.unsafeApply(i), Set(stranger))
      )
      recorded <- rig.recorded
    } yield expect.all(attempts.forall(_.isEmpty), recorded.isEmpty)
  }

  // ===========================================================================
  // Test 3 — Empty pending snapshots
  // ===========================================================================

  test("empty pending snapshots: empty input ⇒ produce returns None even at σ=1") { res =>
    implicit val (h, sp, ssl) = res
    for {
      rig <- freshRig
      gl0Eta = randomGl0Eta()
      shardEta <- ssl.computeShardEta(shardZero, gl0Eta)
      producer <- makeProducer(ssl, rig, Ratio.One, shardEta)
      out <- producer.produce(SortedMap.empty, mkOrd(1L), EtaPeriod(0L), Slot.unsafeApply(1L), Set(rig.selfPeerId))
      recorded <- rig.recorded
    } yield expect.all(out.isEmpty, recorded.isEmpty)
  }

  // ===========================================================================
  // Test 4 — Parent chain-link
  // ===========================================================================

  test("parent chain-link: produced checkpoint's parentCheckpointHash matches bestTip of ShardChainStore") { res =>
    implicit val (h, sp, ssl) = res
    for {
      rig <- freshRig
      gl0Eta = randomGl0Eta()
      shardEta <- ssl.computeShardEta(shardZero, gl0Eta)
      producer <- makeProducer(ssl, rig, Ratio.One, shardEta)
      // Produce one — its binaries are genesis-anchored (Hash.empty), then insert it into the chain store so the next produce sees a
      // non-empty tip. (R-2: the FIRST round's pending must chain off the empty perMgTip ⇒ Hash.empty-anchored binaries.)
      first <- tryProduceUntilSome(
        producer = producer,
        startOrd = 2000L,
        epoch = EtaPeriod(0L),
        maxAttempts = 100,
        committee = Set(rig.selfPeerId),
        pending = mkPendingChainedOff(SortedMap.empty, round = 0)
      )
      firstCp <- IO.fromOption(first)(new RuntimeException("produce-1 should win at σ=1 within 100 attempts"))
      _ <- insertIntoStore(rig.chainStore, firstCp, parentHash = Hash.empty, slot = 1L)
      tipAfterFirst <- rig.chainStore.bestTip
      firstHash = tipAfterFirst.get.hash
      // R-2: the second round's pending binaries must now chain off the SHARD's advanced perMgTip (the first round's included-binary
      // hashes), or the producer's chain-link gate omits them and produce returns None.
      perMgTipAfterFirst <- rig.chainStore.perMgTip
      second <- tryProduceUntilSome(
        producer = producer,
        startOrd = 2100L,
        epoch = EtaPeriod(0L),
        maxAttempts = 100,
        committee = Set(rig.selfPeerId),
        pending = mkPendingChainedOff(perMgTipAfterFirst, round = 1)
      )
      secondCp <- IO.fromOption(second)(new RuntimeException("produce-2 should win at σ=1 within 100 attempts"))
    } yield
      expect.all(
        firstCp.value.parentCheckpointHash == Hash.empty, // genesis
        secondCp.value.parentCheckpointHash == firstHash,
        secondCp.value.shardOrdinal == ShardOrdinal(2L)
      )
  }

  // ===========================================================================
  // Test 4b — S2 BASE-ANCHORED window (VERSION-MODEL §4): chainLinkOrder anchors
  // on the gl0 FINALIZED-base per-MG tip (not the bestTip-derived perMgTip), so
  // the window covers base->latest, RE-INCLUDING adopted-but-unfinalized binaries.
  // ===========================================================================

  test("S2 base-anchored window: anchors on finalizedBasePerMgTip (NOT perMgTip) ⇒ re-includes base->latest binaries") { res =>
    implicit val (h, sp, ssl) = res
    import io.constellationnetwork.security.signature.Signed.SignedOps
    for {
      rig <- freshRig
      gl0Eta = randomGl0Eta()
      shardEta <- ssl.computeShardEta(shardZero, gl0Eta)
      mg = mkAddress("mg-0")
      // Chain b0 (off genesis) -> b1 (off b0); both buffered in round 1.
      b0 = mkSignedBinary("mg-0", 0, parent = Hash.empty)
      b0Hash <- SignedOps(b0).toHashed[IO].map(_.hash)
      b1 = mkSignedBinary("mg-0", 1, parent = b0Hash)
      // Producer with the FINALIZED base STUCK at genesis (empty ⇒ Hash.empty anchor) — DISTINCT from (below) perMgTip after round 0.
      producer <- makeProducer(
        ssl,
        rig,
        Ratio.One,
        shardEta,
        finalizedBaseTip = Some(IO.pure(SortedMap.empty[Address, Hash](Address.OrderingInstance)))
      )
      // Round 0: mint a genesis checkpoint including b0, insert it ⇒ chainStore.perMgTip = hash(b0).
      first <- tryProduceUntilSome(
        producer = producer,
        startOrd = 2000L,
        epoch = EtaPeriod(0L),
        maxAttempts = 100,
        committee = Set(rig.selfPeerId),
        pending = SortedMap(mg -> NonEmptyList.of(b0))(Address.OrderingInstance)
      )
      firstCp <- IO.fromOption(first)(new RuntimeException("produce-1 should win at σ=1 within 100 attempts"))
      _ <- insertIntoStore(rig.chainStore, firstCp, parentHash = Hash.empty, slot = 1L)
      perMgTipAfter <- rig.chainStore.perMgTip
      // Round 1: the buffer RE-INCLUDES b0 (already at perMgTip) plus the new b1. A perMgTip-anchored producer (anchor = hash(b0)) would
      // unfold ONLY [b1]; the base-anchored producer (anchor = genesis Hash.empty) unfolds [b0, b1] — re-including b0.
      second <- tryProduceUntilSome(
        producer = producer,
        startOrd = 2100L,
        epoch = EtaPeriod(0L),
        maxAttempts = 100,
        committee = Set(rig.selfPeerId),
        pending = SortedMap(mg -> NonEmptyList.of(b0, b1))(Address.OrderingInstance)
      )
      secondCp <- IO.fromOption(second)(new RuntimeException("produce-2 should win at σ=1 within 100 attempts"))
      included = secondCp.value.derivedStateDelta.includedSnapshots.get(mg).map(_.toList).getOrElse(Nil)
    } yield
      expect.all(
        // perMgTip advanced to hash(b0) after round 0 — proves the base tip (genesis) we passed is DISTINCT from perMgTip.
        perMgTipAfter.get(mg).contains(b0Hash),
        // Base-anchored window RE-INCLUDES b0 AND b1 (perMgTip-anchoring at hash(b0) would yield ONLY b1).
        included.size == 2,
        included.map(_.value.lastSnapshotHash) == List(Hash.empty, b0Hash)
      )
  }

  // ===========================================================================
  // Test 4b — Newness gate (S2-deadlock fix, runs 19-22): omit stale re-includes (window fully adopted by gl0)
  // ===========================================================================

  test(
    "newness gate: an MG whose base-anchored window holds NOTHING past gl0's adopt tip is OMITTED (stale re-include) ⇒ produce-skip; " +
      "an adopt tip BEHIND the tail ⇒ included with the full base->latest window"
  ) { res =>
    implicit val (h, sp, ssl) = res
    import io.constellationnetwork.security.signature.Signed.SignedOps
    for {
      rig <- freshRig
      gl0Eta = randomGl0Eta()
      shardEta <- ssl.computeShardEta(shardZero, gl0Eta)
      mg = mkAddress("mg-0")
      // Base = genesis ⇒ chainLinkOrder unfolds the window [b0, b1]: b0 off genesis, b1 off b0.
      b0 = mkSignedBinary("mg-0", 0, parent = Hash.empty)
      b0Hash <- SignedOps(b0).toHashed[IO].map(_.hash)
      b1 = mkSignedBinary("mg-0", 1, parent = b0Hash)
      b1Hash <- SignedOps(b1).toHashed[IO].map(_.hash)
      pending = SortedMap(mg -> NonEmptyList.of(b0, b1))(Address.OrderingInstance)
      base = SortedMap.empty[Address, Hash](Address.OrderingInstance) // finalized base = genesis
      // committee={self} ⇒ staircase duty (k=1) always lands on self, so a None return is UNAMBIGUOUSLY the newness gate, never a lost
      // duty draw. STALE: gl0 has already adopted up to b1 (the window's TAIL) ⇒ no binary in [b0,b1] carries lastSnapshotHash==hash(b1)
      // ⇒ the sole MG is omitted ⇒ produce-skip (nothing-new-past-adopt-tip) on every slot.
      staleProducer <- makeProducer(
        ssl,
        rig,
        Ratio.One,
        shardEta,
        finalizedBaseTip = Some(IO.pure(base)),
        adoptedTip = Some(IO.pure(SortedMap(mg -> b1Hash)(Address.OrderingInstance)))
      )
      staleAttempts <- (1L to 12L).toList.traverse { i =>
        staleProducer.produce(pending, mkOrd(2200L + i), EtaPeriod(0L), Slot.unsafeApply(2200L + i), Set(rig.selfPeerId))
      }
      // CONTROL / NEW: gl0 adopted only up to b0 ⇒ b1 (lastSnapshotHash==hash(b0)) is genuinely new ⇒ the MG is included, and the window
      // STILL re-includes b0 (base-anchored, §4 diff intact). Same buffer/base/committee/slots — only the adopt tip differs from STALE.
      newProducer <- makeProducer(
        ssl,
        rig,
        Ratio.One,
        shardEta,
        finalizedBaseTip = Some(IO.pure(base)),
        adoptedTip = Some(IO.pure(SortedMap(mg -> b0Hash)(Address.OrderingInstance)))
      )
      newResult <- tryProduceUntilSome(
        producer = newProducer,
        startOrd = 2300L,
        epoch = EtaPeriod(0L),
        maxAttempts = 100,
        committee = Set(rig.selfPeerId),
        pending = pending
      )
      newCp <- IO.fromOption(newResult)(new RuntimeException("newness gate (new): should win at σ=1 within 100 attempts"))
      included = newCp.value.derivedStateDelta.includedSnapshots.get(mg).map(_.toList).getOrElse(Nil)
    } yield
      expect.all(
        // STALE: every slot returns None — the sole MG's window is fully adopted, nothing past the adopt tip for gl0 to embed.
        staleAttempts.forall(_.isEmpty),
        // NEW: the MG is included AND the window still re-includes b0->b1 (proving the gate decides inclusion only, never trims §4 content).
        included.map(_.value.lastSnapshotHash) == List(Hash.empty, b0Hash)
      )
  }

  // ===========================================================================
  // Test 5 — Shard ordinal monotonicity
  // ===========================================================================

  test("shard ordinal monotonicity: after producing N, the next produce yields N+1") { res =>
    implicit val (h, sp, ssl) = res
    for {
      rig <- freshRig
      gl0Eta = randomGl0Eta()
      shardEta <- ssl.computeShardEta(shardZero, gl0Eta)
      producer <- makeProducer(ssl, rig, Ratio.One, shardEta)
      // Produce 3 in sequence; ordinals should be 1, 2, 3. After each produce, install the result in the chain store so the next call
      // observes the updated tip. R-2: each round's pending binaries chain off the SHARD's current perMgTip (genesis on round 0).
      ords <- (0 until 3).toList
        .foldLeftM[IO, (List[Long], Hash)]((List.empty, Hash.empty)) {
          case ((acc, parentHash), i) =>
            for {
              perMgTip <- rig.chainStore.perMgTip
              produced <- tryProduceUntilSome(
                producer = producer,
                startOrd = 3000L + i * 100L,
                epoch = EtaPeriod(0L),
                maxAttempts = 100,
                committee = Set(rig.selfPeerId),
                pending = mkPendingChainedOff(perMgTip, round = i)
              )
              cp <- IO.fromOption(produced)(new RuntimeException(s"produce-${i + 1} should win at σ=1 within 100 attempts"))
              _ <- insertIntoStore(rig.chainStore, cp, parentHash = parentHash, slot = i.toLong + 1L)
              tip <- rig.chainStore.bestTip
              tipHash = tip.get.hash
            } yield (acc :+ cp.value.shardOrdinal.value, tipHash)
        }
        .map(_._1)
    } yield expect.all(ords == List(1L, 2L, 3L))
  }

  // ===========================================================================
  // Test 6 — derivePerMgState invoked per MG
  // ===========================================================================

  test("derivePerMgState invoked per MG: callback called once per MG, perMetagraphMptRoots has entries per MG") { res =>
    implicit val (h, sp, ssl) = res
    for {
      rig <- freshRig
      gl0Eta = randomGl0Eta()
      shardEta <- ssl.computeShardEta(shardZero, gl0Eta)
      seen <- cats.effect.kernel.Ref.of[IO, List[Address]](List.empty)
      producer <- makeProducer(ssl, rig, Ratio.One, shardEta, derive = recordingDerive(seen))
      pending = mkPendingSnapshots(numMgs = 4)
      result <- tryProduceUntilSome(
        producer,
        startOrd = 4000L,
        EtaPeriod(0L),
        pending = pending,
        maxAttempts = 100,
        committee = Set(rig.selfPeerId)
      )
      cp <- IO.fromOption(result)(new RuntimeException("produce should win at σ=1 within 100 attempts"))
      seenList <- seen.get
    } yield
      expect.all(
        seenList.size == pending.keys.size,
        seenList.toSet == pending.keys.toSet,
        cp.value.derivedStateDelta.perMetagraphMptRoots.size == pending.keys.size,
        cp.value.derivedStateDelta.perMetagraphMptRoots.keys.toSet == pending.keys.toSet,
        // includedSnapshots is the chain-link-ordered input (R-2). For single-binary genesis-anchored MGs the order
        // equals the input, so it round-trips to `pending`.
        cp.value.derivedStateDelta.includedSnapshots == pending,
        // Other delta fields are empty in slice 8 (wired in slice 9/13 when real derivation closure is plugged in).
        cp.value.derivedStateDelta.tokenLockBalancesDelta.isEmpty,
        cp.value.derivedStateDelta.perMetagraphArtifacts.isEmpty,
        cp.value.derivedStateDelta.perMetagraphSyncDataDelta.isEmpty
      )
  }

  test(
    "defer-the-whole-checkpoint-on-OMIT: any active MG that can't derive (None) ⇒ the producer defers the WHOLE checkpoint (produce=None), never a partial one (run-27d deadlock fix); with all MGs derivable it mints a complete checkpoint"
  ) { res =>
    implicit val (h, sp, ssl) = res
    for {
      rigA <- freshRig
      gl0Eta = randomGl0Eta()
      shardEta <- ssl.computeShardEta(shardZero, gl0Eta)
      pending = mkPendingSnapshots(numMgs = 4)
      omittedMg = mkAddress("mg-1") // "can't derive" this round ⇒ None (the genesis-bootstrap adopt-vs-derive race)
      // (A) one un-derivable MG ⇒ DEFER the whole checkpoint (a partial OMITting checkpoint could win fork-choice and wedge the MG)
      producerDefer <- makeProducer(
        ssl,
        rigA,
        Ratio.One,
        shardEta,
        derive =
          (mg, snaps, anchor) => if (mg == omittedMg) IO.pure(None: Option[(Hash, ChangeSet)]) else deterministicDerive(mg, snaps, anchor)
      )
      deferred <- tryProduceUntilSome(
        producerDefer,
        startOrd = 4100L,
        EtaPeriod(0L),
        pending = pending,
        maxAttempts = 20,
        committee = Set(rigA.selfPeerId)
      )
      // (B) all MGs derivable ⇒ COMPLETE checkpoint
      rigB <- freshRig
      producerAll <- makeProducer(ssl, rigB, Ratio.One, shardEta, derive = deterministicDerive)
      complete <- tryProduceUntilSome(
        producerAll,
        startOrd = 4100L,
        EtaPeriod(0L),
        pending = pending,
        maxAttempts = 100,
        committee = Set(rigB.selfPeerId)
      )
      cp <- IO.fromOption(complete)(new RuntimeException("produce should win at σ=1 when all MGs derive"))
    } yield {
      val d = cp.value.derivedStateDelta
      expect.all(
        // (A) on-duty (σ=1) but one MG can't derive ⇒ the producer mints NOTHING (defer), so produce returns None on every attempt
        deferred.isEmpty,
        // (B) all derived ⇒ a COMPLETE checkpoint: all 4 MGs present in all three per-MG fields
        d.perMetagraphMptRoots.keys.toSet == pending.keys.toSet,
        d.perMetagraphStateDiff.keys.toSet == pending.keys.toSet,
        d.includedSnapshots.keys.toSet == pending.keys.toSet,
        d.perMetagraphMptRoots.size == 4
      )
    }
  }

  // ===========================================================================
  // Test 7 — Signature verifies
  // ===========================================================================

  test("signature verifies: the attached CommitteeMemberSignature.ed25519Sig verifies under the test's Ed25519 VK") { res =>
    implicit val (h, sp, ssl) = res
    import io.constellationnetwork.schema.ID.Id
    import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
    for {
      rig <- freshRig
      gl0Eta = randomGl0Eta()
      shardEta <- ssl.computeShardEta(shardZero, gl0Eta)
      producer <- makeProducer(ssl, rig, Ratio.One, shardEta)
      result <- tryProduceUntilSome(producer, startOrd = 5000L, EtaPeriod(0L), maxAttempts = 100, committee = Set(rig.selfPeerId))
      cp <- IO.fromOption(result)(new RuntimeException("produce should win at σ=1 within 100 attempts"))
      // Re-derive the canonical preimage hash (Hasher of ShardCheckpointSigPreimage) — this is the bytes the inner committee sig covers.
      preimageHash <- Hasher[IO].hash(cp.value.signingPreimage)
      // Re-build the SignatureProof shape that signature.verifySignatureProof expects: Id of the operator + Signature(hex of the bytes).
      // The CommitteeMemberSignature.ed25519Sig is hex of the raw signature bytes (as `Signing.signData` produced them).
      committeeSig = cp.value.committeeSignatures.head
      // The signature in the inner CommitteeMemberSignature was signed over `preimageHash.getBytes` via Signing.signData — that is the
      // same byte shape that `signature.verifySignatureProof(hash, ...)` consumes (`signature.verifySignatureProof` calls
      // `Signing.verifySignature(hash.getBytes, sigBytes)`). So we wrap the hex'd signature bytes back into the SignatureProof shape and
      // call the existing verifier.
      proofShape = SignatureProof(Id(rig.selfPeerId.value), Signature(committeeSig.ed25519Sig))
      verified <- verifySignatureProof[IO](preimageHash, proofShape)
      // The outer Signed envelope's SignatureProof is also signed by the same keypair — assert it carries exactly one proof and that
      // proof's signer Id matches the selfPeerId.
      outerProofs = cp.proofs.toNonEmptyList.toList
    } yield
      expect.all(
        verified,
        committeeSig.peerId == rig.selfPeerId,
        outerProofs.size == 1,
        outerProofs.head.id == Id(rig.selfPeerId.value),
        // KES signature: producer copies bytes verbatim from the stub signer (slice 8 doesn't run KES verification).
        committeeSig.kesProductSig == Hex.fromBytes(fixedKesPayload),
        committeeSig.kesTreeStep == 7
      )
  }

  // ===========================================================================
  // Bonus: ShardCheckpointPublisher.recording records every published checkpoint
  // ===========================================================================

  test("recording publisher: every published checkpoint is appended in order") { res =>
    implicit val (h, sp, _) = res
    for {
      pubPair <- ShardCheckpointPublisher.recording[IO]
      (pub, read) = pubPair
      // Build two minimal test envelopes (skip real signing — this test only exercises the publisher mechanics).
      cp1 = stubCheckpoint(1L)
      cp2 = stubCheckpoint(2L)
      _ <- pub.publish(cp1)
      _ <- pub.publish(cp2)
      recorded <- read
    } yield
      expect.all(
        recorded.size == 2,
        recorded.head.value.shardOrdinal == ShardOrdinal(1L),
        recorded(1).value.shardOrdinal == ShardOrdinal(2L)
      )
  }

  test("noop publisher: publish is a no-op (returns successfully without side effects)") { _ =>
    val pub = ShardCheckpointPublisher.noop[IO]
    pub.publish(stubCheckpoint(99L)).map(_ => expect(true)) // sanity: doesn't throw
  }

  // ===========================================================================
  // Tier-1 idempotence (task #45, run-19): a stuck ordinal is minted ONCE and
  // re-published (cadence-gated) — NOT re-minted with a churning gl0Anchor+slot.
  // ===========================================================================

  test("task #45 idempotence: a stuck shardOrdinal is minted ONCE and re-published — no anchor/hash churn") { res =>
    implicit val (h, sp, ssl) = res
    for {
      rig <- freshRig
      gl0Eta = randomGl0Eta()
      shardEta <- ssl.computeShardEta(shardZero, gl0Eta)
      // republishEveryTicks=1 ⇒ every memo-hit re-publishes. Single-member committee ⇒ always on duty, so the first produce mints.
      producer <- makeProducer(ssl, rig, Ratio.One, shardEta, republishEveryTicks = 1)
      // Drive 8 produces with ADVANCING gl0Anchor (2100..2107) + slot, and NEVER insert into the store, so bestTip stays None ⇒
      // nextShardOrdinal stays ord-1 ⇒ every tick after the first is a memo HIT. Pre-fix, each tick re-minted a fresh hash anchored to
      // the advancing gl0Anchor (run-19: 34 variants). Post-fix, every emitted checkpoint carries the gl0Anchor PINNED at first mint.
      results <- (2100L to 2107L).toList.traverse(i =>
        producer.produce(mkPendingSnapshots(2), mkOrd(i), EtaPeriod(0L), Slot.unsafeApply(i), Set(rig.selfPeerId))
      )
      somes = results.flatten
      hashes <- somes.traverse(s => Hasher[IO].hash(s.value.signingPreimage))
      recorded <- rig.recorded
      recordedHashes <- recorded.traverse(s => Hasher[IO].hash(s.value.signingPreimage))
    } yield
      expect.all(
        somes.size == 8, // mint (i=2100) + 7 re-publishes (single-member committee ⇒ always on duty)
        hashes.distinct.size == 1, // EXACTLY ONE checkpoint identity across all returns — the anti-churn invariant
        somes.map(_.value.gl0AnchorOrdinal).distinct == List(mkOrd(2100L)), // anchor PINNED at first mint, NOT churning 2100..2107
        somes.forall(_.value.shardOrdinal == ShardOrdinal(1L)),
        somes.forall(_.value.slot.value.value == 2100L), // slot pinned too
        recorded.size == 8,
        recordedHashes.distinct.size == 1 // every on-wire publish is the SAME bytes
      )
  }

  test("task #45 cadence: with republishEveryTicks=3, a held checkpoint re-publishes only every 3rd produce tick") { res =>
    implicit val (h, sp, ssl) = res
    for {
      rig <- freshRig
      gl0Eta = randomGl0Eta()
      shardEta <- ssl.computeShardEta(shardZero, gl0Eta)
      producer <- makeProducer(ssl, rig, Ratio.One, shardEta, republishEveryTicks = 3)
      // 7 produces (ticks 1..7), ordinal stuck (never inserted). Mint at tick 1 (lastPublished=1); re-publish when
      // tick - lastPublished >= 3 ⇒ ticks 4 and 7. So publishes at ticks {1,4,7} = 3; ticks {2,3,5,6} are cadence-wait (None).
      results <- (3100L to 3106L).toList.traverse(i =>
        producer.produce(mkPendingSnapshots(2), mkOrd(i), EtaPeriod(0L), Slot.unsafeApply(i), Set(rig.selfPeerId))
      )
      recorded <- rig.recorded
      recordedHashes <- recorded.traverse(s => Hasher[IO].hash(s.value.signingPreimage))
    } yield
      expect.all(
        results.flatten.size == 3, // Somes only on the publish ticks {1,4,7}
        results.count(_.isEmpty) == 4, // cadence-wait Nones on ticks {2,3,5,6}
        recorded.size == 3,
        recordedHashes.distinct.size == 1 // all the same held bytes
      )
  }

  test("task #45 advancement: the memo does NOT block the next ordinal — after the tip advances, produce mints N+1") { res =>
    implicit val (h, sp, ssl) = res
    for {
      rig <- freshRig
      gl0Eta = randomGl0Eta()
      shardEta <- ssl.computeShardEta(shardZero, gl0Eta)
      producer <- makeProducer(ssl, rig, Ratio.One, shardEta, republishEveryTicks = 1)
      // Mint ord-1 (held), then INSERT it so bestTip advances to ord-1. The next produce sees nextShardOrdinal=2 ⇒ the held ord-1 is a
      // MISS (different ordinal) ⇒ the memo is dropped and ord-2 is minted (a new, distinct hash). Proves the memo never wedges progress.
      first <- tryProduceUntilSome(
        producer,
        startOrd = 4000L,
        epoch = EtaPeriod(0L),
        maxAttempts = 100,
        committee = Set(rig.selfPeerId),
        pending = mkPendingChainedOff(SortedMap.empty, round = 0)
      )
      firstCp <- IO.fromOption(first)(new RuntimeException("produce-1 should win at σ=1 within 100 attempts"))
      _ <- insertIntoStore(rig.chainStore, firstCp, parentHash = Hash.empty, slot = 4000L)
      perMgTip <- rig.chainStore.perMgTip
      second <- tryProduceUntilSome(
        producer,
        startOrd = 4100L,
        epoch = EtaPeriod(0L),
        maxAttempts = 100,
        committee = Set(rig.selfPeerId),
        pending = mkPendingChainedOff(perMgTip, round = 1)
      )
      secondCp <- IO.fromOption(second)(new RuntimeException("produce-2 should win and advance to ord-2"))
      firstHash <- Hasher[IO].hash(firstCp.value.signingPreimage)
      secondHash <- Hasher[IO].hash(secondCp.value.signingPreimage)
    } yield
      expect.all(
        firstCp.value.shardOrdinal == ShardOrdinal(1L),
        secondCp.value.shardOrdinal == ShardOrdinal(2L),
        secondCp.value.parentCheckpointHash == firstHash, // ord-2 chains off ord-1
        firstHash =!= secondHash // a genuinely new mint, not a stale re-publish of ord-1
      )
  }

  // ===========================================================================
  // Helpers
  // ===========================================================================

  /** Coerce a Long into a `SnapshotOrdinal`. Helper to keep call sites readable. */
  private def mkOrd(value: Long): SnapshotOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(value))

  /** Loop until `produce(...)` returns `Some` or `maxAttempts` is reached. We iterate over `startOrd` upward — each iteration is a
    * different `(slot, gl0AnchorOrdinal)` pair, which gives a fresh VRF input and therefore a fresh draw. At σ=1 with the LDD recovery
    * regime, the draw saturates and the first try usually wins; the defensive loop is there for the rare miss when the test seed lands the
    * VRF output near the threshold.
    */
  private def tryProduceUntilSome(
    producer: ShardCheckpointProducer[IO],
    startOrd: Long,
    epoch: EtaPeriod,
    maxAttempts: Int,
    committee: Set[PeerId],
    pending: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]] = mkPendingSnapshots(2)
  ): IO[Option[Signed[ShardCheckpoint]]] = {
    def loop(attempt: Int): IO[Option[Signed[ShardCheckpoint]]] =
      if (attempt >= maxAttempts) IO.pure(None)
      else
        producer
          .produce(pending, mkOrd(startOrd + attempt.toLong), epoch, Slot.unsafeApply(startOrd + attempt.toLong), committee)
          .flatMap {
            case s @ Some(_) => IO.pure(s)
            case None        => loop(attempt + 1)
          }
    loop(0)
  }

  /** Helper to insert a produced envelope into a `ShardChainStore`. Mirrors the store contract: pass the envelope + parentHash + ord + slot
    * + a deterministic vrfOutput stub. Slice 8 tests don't exercise fork choice; vrfOutput choice doesn't matter as long as it's stable.
    */
  private def insertIntoStore(
    store: ShardChainStore[IO],
    signed: Signed[ShardCheckpoint],
    parentHash: Hash,
    slot: Long
  ): IO[Unit] =
    store
      .store(
        checkpoint = signed,
        parentHash = parentHash,
        shardOrdinal = signed.value.shardOrdinal,
        slot = slot,
        vrfOutput = Array.fill[Byte](32)(0x42.toByte)
      )
      .void

  /** Minimal stub checkpoint — same shape as the chain-store tests use. Only the `shardOrdinal` field varies for the publisher tests. */
  private def stubCheckpoint(ord: Long): Signed[ShardCheckpoint] = {
    import cats.data.NonEmptySet
    import io.constellationnetwork.schema.ID.Id
    import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
    val cp = ShardCheckpoint(
      shardId = shardZero,
      parentCheckpointHash = Hash.empty,
      shardOrdinal = ShardOrdinal(ord),
      gl0AnchorOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(0L)),
      slot = SlotT.unsafeApply(0L),
      derivedStateDelta = ShardDerivedStateDelta.empty,
      emittedReceipts = List.empty,
      committeeSignatures = NonEmptyList.of(
        CommitteeMemberSignature(
          peerId = PeerId(Hex("aa" * 64)),
          vrfProof = Hex("bb" * 80),
          ed25519Sig = Hex("cc" * 64),
          kesProductSig = Hex("dd" * 128),
          kesTreeStep = 0
        )
      ),
      epoch = EtaPeriod(0L)
    )
    val proof = SignatureProof(Id(Hex("ee" * 64)), Signature(Hex("ff" * 70)))
    Signed(cp, NonEmptySet.of(proof))
  }
}
