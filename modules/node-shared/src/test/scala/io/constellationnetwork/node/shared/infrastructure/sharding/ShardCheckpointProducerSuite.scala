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
import io.constellationnetwork.node.shared.domain.nakamoto.sharding.{ShardChainStore, ShardSlotLeader}
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

  /** Deterministic `derivePerMgState` that returns a `Hash` derived from `(mg, headBinary.lastSnapshotHash)`. Lets tests assert on the
    * per-MG hash values present in the produced `derivedStateDelta.perMetagraphMptRoots`.
    */
  private def deterministicDerive(
    mg: Address,
    snaps: NonEmptyList[Signed[StateChannelSnapshotBinary]],
    anchor: SnapshotOrdinal
  ): IO[Hash] = {
    val _ = anchor
    IO.pure(hashFromString(s"derived-${mg.value.value}-${snaps.head.value.lastSnapshotHash.value.take(8)}"))
  }

  /** A `derivePerMgState` callback that records which MGs it was invoked for. The test uses this to assert callback invocation count and
    * argument coverage matches the input map.
    */
  private def recordingDerive(
    seen: cats.effect.kernel.Ref[IO, List[Address]]
  ): (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal) => IO[Hash] =
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
    derive: (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal) => IO[Hash] = deterministicDerive
  )(implicit h: Hasher[IO], sp: SecurityProvider[IO]): IO[ShardCheckpointProducer[IO]] =
    ShardCheckpointProducer.make[IO](
      shardId = shardZero,
      chainStore = rig.chainStore,
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
      sigmaInCommittee = sigma,
      slotForGl0Anchor = slotForGl0Anchor,
      slotGapFor = slotGapFor,
      lddConfig = LddConfig.Default,
      derivePerMgState = derive
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
      result <- tryProduceUntilSome(producer, startOrd = 1000L, EtaPeriod(0L), maxAttempts = 100)
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

  test("skip when not leader: σ=0 ⇒ produce returns None, publisher not called") { res =>
    implicit val (h, sp, ssl) = res
    for {
      rig <- freshRig
      gl0Eta = randomGl0Eta()
      shardEta <- ssl.computeShardEta(shardZero, gl0Eta)
      // σ=0 ⇒ EligibilityChecker.threshold returns 0 ⇒ no slot ever wins (contract documented in EligibilityChecker.threshold).
      producer <- makeProducer(ssl, rig, Ratio.Zero, shardEta)
      // Try several gl0 anchors — must all return None.
      attempts <- (1L to 10L).toList.traverse(i => producer.produce(mkPendingSnapshots(2), mkOrd(i), EtaPeriod(0L)))
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
      out <- producer.produce(SortedMap.empty, mkOrd(1L), EtaPeriod(0L))
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
        producer,
        startOrd = 2000L,
        EtaPeriod(0L),
        maxAttempts = 100,
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
        producer,
        startOrd = 2100L,
        EtaPeriod(0L),
        maxAttempts = 100,
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
                producer,
                startOrd = 3000L + i * 100L,
                EtaPeriod(0L),
                maxAttempts = 100,
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
      result <- tryProduceUntilSome(producer, startOrd = 4000L, EtaPeriod(0L), pending = pending, maxAttempts = 100)
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
      result <- tryProduceUntilSome(producer, startOrd = 5000L, EtaPeriod(0L), maxAttempts = 100)
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
    pending: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]] = mkPendingSnapshots(2)
  ): IO[Option[Signed[ShardCheckpoint]]] = {
    def loop(attempt: Int): IO[Option[Signed[ShardCheckpoint]]] =
      if (attempt >= maxAttempts) IO.pure(None)
      else
        producer.produce(pending, mkOrd(startOrd + attempt.toLong), epoch).flatMap {
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
