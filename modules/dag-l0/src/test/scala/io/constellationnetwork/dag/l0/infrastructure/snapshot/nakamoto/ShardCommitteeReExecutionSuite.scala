package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import java.security.KeyPair

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.currency.schema.currency.{CurrencySnapshot, SnapshotFee}
import io.constellationnetwork.dag.l0.infrastructure.snapshot.GlobalSnapshotStateChannelEventsProcessorSuite
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.EligibilityChecker
import io.constellationnetwork.node.shared.domain.nakamoto.sharding._
import io.constellationnetwork.node.shared.infrastructure.metrics.{Metrics, NoOpMetrics}
import io.constellationnetwork.node.shared.infrastructure.sharding.{
  ShardCheckpointProducer,
  ShardCheckpointPublisher,
  ShardCheckpointWiring
}
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.{
  ShardCheckpointAcceptResult,
  ShardCheckpointGl0AcceptanceManager
}
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
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.Signed.forAsyncHasher
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}
import io.constellationnetwork.shared.sharedKryoRegistrar
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** S3 — committee re-execution byte-identity contract (`docs/nakamoto/SHARD-SORTITION-WORKSTREAM-PLAN.md` slice S3).
  *
  * The heart of S3 is: an attestation must mean "I independently re-ran this metagraph's derivation and got result R". This suite proves
  * the load-bearing safety property — '''producer-root == verifier-root''' — using the REAL derivation
  * (`GlobalSnapshotStateChannelEventsProcessor.deriveMetagraphRoot` wired via `ShardCheckpointWiring.reExecDerivation`), reusing the same
  * heavyweight real processor `GlobalSnapshotStateChannelEventsProcessorSuite.mkProcessor` builds (the SAME processor gl0 uses for
  * metagraph snapshots).
  *
  *   1. '''Golden byte-identity''': a producer wired with the real re-exec closure builds a checkpoint with a REAL per-MG root; the
  *      verifier's `reExecuteDerivation` (the SAME closure) over the SAME `includedSnapshots` at the SAME `gl0AnchorOrdinal` recomputes the
  *      IDENTICAL `Hash` ⇒ on the degraded `T_depth1` path the acceptance manager `Accepts`.
  *   1. '''Tampered delta''': a checkpoint whose `perMetagraphMptRoots` is altered (≠ re-execution) ⇒ `RejectedReExecutionMismatch` with
  *      the FULL `slashSigners` list (not short-circuited).
  *   1. '''Inversion gate''': `NakamotoSyncDaemon.shardCheckpointAdmissible` — a re-exec-FAILED result is NOT admissible (not adopted as
  *      best-tip, no attestation emitted), an `Accepted`/`Pending` result is.
  *
  * Both branches of `deriveMetagraphRoot` are exercised: a real serialized `Signed[CurrencySnapshot]` genesis binary (the meaningful
  * full-snapshot leaf) and a non-currency binary (the address-only sentinel).
  */
object ShardCommitteeReExecutionSuite extends MutableIOSuite {

  override type Res = (KryoSerializer[IO], Hasher[IO], JsonSerializer[IO], SecurityProvider[IO], ShardSlotLeader[IO])

  implicit val metrics: Metrics[IO] = NoOpMetrics.make

  override def sharedResource: Resource[IO, Res] =
    for {
      implicit0(ks: KryoSerializer[IO]) <- KryoSerializer.forAsync[IO](sharedKryoRegistrar)
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
      log1p <- Log1pInterpreter.make[IO](maxIterations = 10000, precision = 8).asResource
      exp <- ExpInterpreter.make[IO](maxIterations = 10000, precision = 38).asResource
      ec = EligibilityChecker.make[IO](log1p, exp)
      ssl = ShardSlotLeader.make[IO](ec)
    } yield (ks, h, j, sp, ssl)

  private val shardZero: ShardId = ShardId.unsafeApply(0)
  private val epochZero: EtaPeriod = EtaPeriod(0L)
  private val anchorOrd: SnapshotOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(1000L))

  private val fixedShardEta: Array[Byte] = Array.fill[Byte](32)(0x7a.toByte)
  private val fixedVrfSk: Array[Byte] = Array.fill[Byte](32)(0x5c.toByte)
  private val fixedKesPayload: Array[Byte] = Array.fill[Byte](128)(0xab.toByte)

  private val slotForGl0Anchor: SnapshotOrdinal => Slot = ord => Slot.unsafeApply(ord.value.value)
  private val slotGapFor: (Slot, Option[Slot]) => Long =
    (cur, parentOpt) => parentOpt.fold(cur.value.value)(p => math.max(1L, cur.value.value - p.value.value))

  /** Build a REAL signed currency-genesis SC binary for `mgKeyPair` — a `Signed[CurrencySnapshot]` JSON-serialized into the binary
    * `content`, exactly the shape `GlobalSnapshotStateChannelEventsProcessor.processCurrencySnapshots` deserializes and derives a state
    * from (so the per-MG root is the meaningful full-snapshot Merkle leaf, not the address-only sentinel).
    */
  private def mkCurrencyGenesisBinary(
    mgKeyPair: KeyPair
  )(implicit h: Hasher[IO], sp: SecurityProvider[IO], j: JsonSerializer[IO]): IO[Signed[StateChannelSnapshotBinary]] = {
    val genesis: CurrencySnapshot = CurrencySnapshot.mkGenesis(Map.empty, None, None)
    for {
      signedGenesis <- forAsyncHasher(genesis, mgKeyPair)
      contentBytes <- JsonSerializer[IO].serialize(signedGenesis)
      binary = StateChannelSnapshotBinary(Hash.empty, contentBytes, SnapshotFee.MinValue)
      signedBinary <- forAsyncHasher(binary, mgKeyPair)
    } yield signedBinary
  }

  /** A non-currency binary — its content is a VALID brotli-serialized JSON value (a plain string, mirroring
    * `GlobalSnapshotStateChannelEventsProcessorSuite.mkStateChannelOutput`) that decompresses cleanly but does NOT decode as a currency
    * snapshot, so the derivation yields no state and the root falls to the deterministic address-only sentinel. Exercises the `None` branch
    * of `deriveMetagraphRoot`.
    */
  private def mkOpaqueBinary(
    mgKeyPair: KeyPair
  )(implicit h: Hasher[IO], sp: SecurityProvider[IO], j: JsonSerializer[IO]): IO[Signed[StateChannelSnapshotBinary]] =
    for {
      contentBytes <- JsonSerializer[IO].serialize("not-a-currency-snapshot")
      binary = StateChannelSnapshotBinary(Hash.empty, contentBytes, SnapshotFee.MinValue)
      signedBinary <- forAsyncHasher(binary, mgKeyPair)
    } yield signedBinary

  /** Build a producer wired with the REAL re-exec closure (σ=1 so it always wins the slot lottery in the recovery regime). */
  private def mkRealProducer(
    ssl: ShardSlotLeader[IO],
    chainStore: ShardChainStore[IO],
    keyPair: KeyPair,
    reExec: (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal) => IO[Hash]
  )(implicit h: Hasher[IO], sp: SecurityProvider[IO]): IO[ShardCheckpointProducer[IO]] =
    ShardCheckpointProducer.make[IO](
      shardId = shardZero,
      chainStore = chainStore,
      slotLeader = ssl,
      publisher = ShardCheckpointPublisher.noop[IO],
      selfPeerId = PeerId.fromPublic(keyPair.getPublic),
      selfKeyPair = keyPair,
      selfVrfSk = fixedVrfSk,
      kesSigner = ShardCheckpointProducer.KesSigner.fixed[IO](period = 7, signatureBytes = fixedKesPayload),
      shardEtaFor = _ => IO.pure(fixedShardEta),
      sigmaInCommittee = Ratio.One,
      slotForGl0Anchor = slotForGl0Anchor,
      slotGapFor = slotGapFor,
      lddConfig = LddConfig.Default,
      derivePerMgState = reExec,
      lastAdoptedOrd = cats.effect.IO.pure(None),
      pipelineDepth = Int.MaxValue
    )

  /** Loop produce over increasing gl0 anchors until σ=1 wins. */
  private def produceUntilSome(
    producer: ShardCheckpointProducer[IO],
    pending: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
    startOrd: Long,
    maxAttempts: Int = 100
  ): IO[Option[Signed[ShardCheckpoint]]] = {
    def loop(attempt: Int): IO[Option[Signed[ShardCheckpoint]]] =
      if (attempt >= maxAttempts) IO.pure(None)
      else
        producer.produce(pending, SnapshotOrdinal(NonNegLong.unsafeFrom(startOrd + attempt.toLong)), epochZero).flatMap {
          case s @ Some(_) => IO.pure(s)
          case None        => loop(attempt + 1)
        }
    loop(0)
  }

  /** Seed a linear chain of `n` checkpoints so `bestTip` resolves high — lets the T_depth1 trigger qualify a low checkpoint ord. */
  private def seedChain(store: ShardChainStore[IO], n: Int): IO[Unit] =
    (0L until n.toLong).toList
      .foldLeftM[IO, Hash](Hash("0" * 64)) {
        case (parent, ord) =>
          val cp = ShardCheckpoint(
            shardId = shardZero,
            parentCheckpointHash = parent,
            shardOrdinal = ShardOrdinal(ord),
            gl0AnchorOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(100L + ord)),
            derivedStateDelta = ShardDerivedStateDelta.empty,
            emittedReceipts = List.empty,
            committeeSignatures = NonEmptyList.of(
              CommitteeMemberSignature(PeerId(Hex(f"${ord.toInt + 1}%02x" * 64)), Hex("aa" * 80), Hex("bb" * 64), Hex("cc" * 128), 0)
            ),
            epoch = epochZero
          )
          val signed =
            Signed(cp, NonEmptySet.of(SignatureProof(io.constellationnetwork.schema.ID.Id(Hex("11" * 64)), Signature(Hex("22" * 70)))))
          store
            .store(signed, parent, ShardOrdinal(ord), ord + 1L, Array.fill[Byte](32)(ord.toByte))
            .flatMap(_ => store.bestTip.map(_.get.hash))
      }
      .void

  /** Build a manager whose T_depth1 trigger qualifies (forcing the re-exec path) and T_count does NOT, wired with the REAL re-exec closure
    * + the checkpoint's signer as a committee member.
    */
  private def mkDepth1Manager(
    reExec: (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal) => IO[Hash],
    committee: Set[PeerId],
    selfId: PeerId
  )(implicit h: Hasher[IO], sp: SecurityProvider[IO]): IO[ShardCheckpointGl0AcceptanceManager[IO]] =
    for {
      store <- ShardChainStore.make[IO](shardZero)
      _ <- seedChain(store, 10)
      tracker <- ShardTipTracker.make[IO](shardZero, selfId)
      // kQuorum huge + zero attestations ⇒ T_count never qualifies; k1Shard=3 vs bestOrd=9 ⇒ T_depth1 qualifies low ords.
      triggers <- ShardFinalityTriggers.make[IO](shardZero, kQuorum = 1000, k1Shard = 3L, store, tracker)
      _ <- triggers.advance
      mgr <- ShardCheckpointGl0AcceptanceManager.make[IO](
        finalityTriggers = sid => IO.pure(if (sid === shardZero) triggers.some else None),
        chainStore = _ => IO.pure(none[ShardChainStore[IO]]),
        committeeMembership = (_, _) => IO.pure(committee),
        kDraw = 1000,
        kQuorum = 1000,
        selfPeerId = selfId,
        kesRegistry = io.constellationnetwork.node.shared.domain.nakamoto.KesRegistry.empty[IO],
        reExecuteDerivation = reExec
      )
    } yield mgr

  // ===========================================================================
  // Test 1 — GOLDEN: producer-root == verifier-root over the SAME real derivation (full-snapshot leaf) ⇒ Accepted
  // ===========================================================================

  test("golden byte-identity (real currency genesis): producer per-MG root == verifier re-exec ⇒ T_depth1 Accepted") { res =>
    implicit val (ks, h, j, sp, ssl) = res
    for {
      processor <- GlobalSnapshotStateChannelEventsProcessorSuite.mkProcessor(Map.empty)
      reExec = ShardCheckpointWiring.reExecDerivation[IO](processor)
      opKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      selfPeer = PeerId.fromPublic(opKeyPair.getPublic)
      mgKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      mgAddr = io.constellationnetwork.security.key.ops.PublicKeyOps(mgKeyPair.getPublic).toAddress
      binary <- mkCurrencyGenesisBinary(mgKeyPair)
      pending = SortedMap(mgAddr -> NonEmptyList.of(binary))(Address.OrderingInstance)

      // PRODUCER builds the checkpoint with a REAL per-MG root via the shared closure.
      store <- ShardChainStore.make[IO](shardZero)
      producer <- mkRealProducer(ssl, store, opKeyPair, reExec)
      producedOpt <- produceUntilSome(producer, pending, startOrd = anchorOrd.value.value)
      produced <- IO.fromOption(producedOpt)(new RuntimeException("σ=1 producer should win within 100 attempts"))
      producedRoot = produced.value.derivedStateDelta.perMetagraphMptRoots(mgAddr)

      // Independently recompute via the SAME closure over the SAME chain + the checkpoint's own gl0AnchorOrdinal.
      verifierRoot <- reExec(mgAddr, NonEmptyList.of(binary), produced.value.gl0AnchorOrdinal)

      // Sanity: the real full-snapshot leaf is NOT the address-only sentinel (i.e. the meaningful branch ran).
      sentinel <- Hasher[IO].hash(mgAddr)

      // VERIFIER acceptance manager re-executes on the degraded T_depth1 path and must Accept (roots match).
      mgr <- mkDepth1Manager(reExec, committee = Set(selfPeer), selfId = selfPeer)
      verifyResult <- mgr.evaluate(produced.value)
    } yield
      expect.all(
        producedRoot === verifierRoot,
        producedRoot =!= sentinel, // exercised the full-snapshot leaf branch, not the no-state fallback
        verifyResult == ShardCheckpointAcceptResult.Accepted
      )
  }

  // ===========================================================================
  // Test 2 — GOLDEN (opaque binary, sentinel branch): producer-root == verifier-root ⇒ Accepted
  // ===========================================================================

  test("golden byte-identity (opaque binary, address sentinel): producer root == verifier re-exec ⇒ Accepted") { res =>
    implicit val (ks, h, j, sp, ssl) = res
    for {
      processor <- GlobalSnapshotStateChannelEventsProcessorSuite.mkProcessor(Map.empty)
      reExec = ShardCheckpointWiring.reExecDerivation[IO](processor)
      opKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      selfPeer = PeerId.fromPublic(opKeyPair.getPublic)
      mgKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      mgAddr = io.constellationnetwork.security.key.ops.PublicKeyOps(mgKeyPair.getPublic).toAddress
      binary <- mkOpaqueBinary(mgKeyPair)
      pending = SortedMap(mgAddr -> NonEmptyList.of(binary))(Address.OrderingInstance)

      store <- ShardChainStore.make[IO](shardZero)
      producer <- mkRealProducer(ssl, store, opKeyPair, reExec)
      producedOpt <- produceUntilSome(producer, pending, startOrd = anchorOrd.value.value)
      produced <- IO.fromOption(producedOpt)(new RuntimeException("σ=1 producer should win within 100 attempts"))
      producedRoot = produced.value.derivedStateDelta.perMetagraphMptRoots(mgAddr)
      sentinel <- Hasher[IO].hash(mgAddr)

      mgr <- mkDepth1Manager(reExec, committee = Set(selfPeer), selfId = selfPeer)
      verifyResult <- mgr.evaluate(produced.value)
    } yield
      expect.all(
        producedRoot === sentinel, // opaque content ⇒ no derived state ⇒ address-only sentinel
        verifyResult == ShardCheckpointAcceptResult.Accepted
      )
  }

  // ===========================================================================
  // Test 3 — TAMPERED delta: perMetagraphMptRoots altered ⇒ RejectedReExecutionMismatch with FULL signer list
  // ===========================================================================

  test("tampered delta: perMetagraphMptRoots ≠ re-execution ⇒ RejectedReExecutionMismatch with full slashSigners") { res =>
    implicit val (ks, h, j, sp, ssl) = res
    for {
      processor <- GlobalSnapshotStateChannelEventsProcessorSuite.mkProcessor(Map.empty)
      reExec = ShardCheckpointWiring.reExecDerivation[IO](processor)
      opKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      selfPeer = PeerId.fromPublic(opKeyPair.getPublic)
      mgKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      mgAddr = io.constellationnetwork.security.key.ops.PublicKeyOps(mgKeyPair.getPublic).toAddress
      binary <- mkCurrencyGenesisBinary(mgKeyPair)
      pending = SortedMap(mgAddr -> NonEmptyList.of(binary))(Address.OrderingInstance)

      store <- ShardChainStore.make[IO](shardZero)
      producer <- mkRealProducer(ssl, store, opKeyPair, reExec)
      producedOpt <- produceUntilSome(producer, pending, startOrd = anchorOrd.value.value)
      produced <- IO.fromOption(producedOpt)(new RuntimeException("σ=1 producer should win within 100 attempts"))

      // TAMPER: overwrite the per-MG root with a wrong value, modelling a MALICIOUS producer that computed a WRONG derivation
      // result. The committee preimage changes, so we RE-SIGN it (a correctly-signed wrong-derivation envelope — the exact
      // §10.2 slashable case). Pre-checks (membership + Ed25519 + KES carve-out + structural VRF) all pass, so the manager
      // reaches the re-exec path and the recomputed root MISMATCHES the tampered claim ⇒ RejectedReExecutionMismatch.
      tamperedDelta = produced.value.derivedStateDelta.copy(perMetagraphMptRoots = SortedMap(mgAddr -> Hash("ff" * 32)))
      tamperedCp0 = produced.value.copy(derivedStateDelta = tamperedDelta)
      tamperedPreimageHash <- Hasher[IO].hash(tamperedCp0.signingPreimage)
      reEdSig <- io.constellationnetwork.security.signature.Signing.signData[IO](tamperedPreimageHash.getBytes)(opKeyPair.getPrivate)
      origSig = produced.value.committeeSignatures.head
      reSig = origSig.copy(ed25519Sig = Hex.fromBytes(reEdSig))
      tamperedCp = tamperedCp0.copy(committeeSignatures = NonEmptyList.of(reSig))

      mgr <- mkDepth1Manager(reExec, committee = Set(selfPeer), selfId = selfPeer)
      verifyResult <- mgr.evaluate(tamperedCp)
    } yield
      verifyResult match {
        case ShardCheckpointAcceptResult.RejectedReExecutionMismatch(reason, slashSigners) =>
          expect.all(
            reason.contains("re-exec mismatch"),
            slashSigners == tamperedCp.committeeSignatures.toList.map(_.peerId)
          )
        case other => failure(s"Expected RejectedReExecutionMismatch, got $other")
      }
  }

  // ===========================================================================
  // Test 4 — INVERSION gate: a re-exec-FAILED result is NOT adopted/attested; Accepted/Pending are
  // ===========================================================================

  test("inversion gate (shardCheckpointAdmissible): reject/mismatch ⇒ not adopted+attested; accepted/pending ⇒ adopted") { _ =>
    val signers = List(PeerId(Hex("aa" * 64)))
    IO.pure(
      expect.all(
        NakamotoSyncDaemon.shardCheckpointAdmissible(ShardCheckpointAcceptResult.Accepted),
        NakamotoSyncDaemon.shardCheckpointAdmissible(ShardCheckpointAcceptResult.PendingMoreAttestations),
        !NakamotoSyncDaemon.shardCheckpointAdmissible(ShardCheckpointAcceptResult.Rejected("bad pre-check")),
        !NakamotoSyncDaemon.shardCheckpointAdmissible(ShardCheckpointAcceptResult.RejectedReExecutionMismatch("mismatch", signers))
      )
    )
  }
}
