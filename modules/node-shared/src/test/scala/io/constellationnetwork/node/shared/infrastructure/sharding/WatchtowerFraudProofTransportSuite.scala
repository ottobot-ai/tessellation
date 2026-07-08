package io.constellationnetwork.node.shared.infrastructure.sharding

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.kernel.Ref
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.SortedMap
import scala.concurrent.duration._

import io.constellationnetwork.currency.schema.currency.SnapshotFee
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.slashing.{InvalidStateProofSlashedReader, InvalidStateProofValidator}
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.SidecarClient
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.proto.sidecar._
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.{
  ShardCheckpointAcceptResult,
  ShardCheckpointGl0AcceptanceManager,
  WatchtowerMismatch
}
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nakamoto.slot.Slot
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding._
import io.constellationnetwork.schema.slashing.InvalidStateProofEvidence
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import eu.timepit.refined.types.numeric.NonNegLong
import io.grpc.ManagedChannel
import weaver.MutableIOSuite

/** FINDING-F2 (EPIC-9-NET M4) — the JVM legs of the watchtower fraud-proof transport, end to end:
  *
  * {{{ emit (re-exec mismatch) → sign → publish (wire) → [sidecar gossip, covered by the Go suite] → fromWire → verdict → pool }}}
  *
  *   - '''emit → publish''': a watchtower mismatch produces exactly one signed `FraudProofEnvelopeWire` on the sidecar client. Pre-fix the
  *     RPC died in the sidecar (UNIMPLEMENTED — no Go handler/topic/relay existed) and the emitter warn-swallowed after ONE attempt, so
  *     the evidence never left the node.
  *   - '''retry-or-outbox''': a transiently-failing publish (sidecar restarting) is retried
  *     (`nakamoto.invalidity-slashing.fraud-proof-publish-*`); total failure is still swallowed after the configured attempts — the
  *     adopt/receive path must never crash on watchtower trouble.
  *   - '''receive → admit''': the captured wire bytes decode via `FraudProofWireCodecs.fromWire` and — exactly as
  *     `NakamotoSyncDaemon.handleFraudProof` does — build an [[InvalidStateProofEvidence]] whose verdict the REAL
  *     [[InvalidStateProofValidator]] upholds (including verifying the emitter's REAL Ed25519 challenger signature), and the upheld
  *     evidence is admitted into the [[WatchtowerFraudProofPool]] the gl0 leader embeds from.
  *
  * The Go-side transport hop (topic join, relay, Subscribe arm, outbox durability) is pinned by
  * `p2p/internal/grpcserver/server_test.go` + `p2p/internal/gossip/gossip_fraud_test.go`; the daemon's dispatch arm exists at
  * `NakamotoSyncDaemon` `case pb.GossipMessage.Body.FraudProof`. NOT covered here: the daemon's chain-store lookup of the disputed
  * checkpoint (needs full `AcceptanceDeps`), and the downstream slash CONSEQUENCE (committee exclusion — FINDING-002, a separate task; the
  * boundary is the pool → leader-embedded `fraudProofs` artifact → GSAM re-validate + `applySlash`).
  */
object WatchtowerFraudProofTransportSuite extends MutableIOSuite {

  override type Res = (Hasher[IO], SecurityProvider[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp)

  // ─── fixtures (mirror InvalidStateProofValidatorSuite's checkpoint shape) ───

  private val mgA: Address = Address.fromBytes("mgA".getBytes("UTF-8"))
  private val attestedRoot: Hash = Hash("a" * 64)
  private val honestRoot: Hash = Hash("b" * 64)

  private def committeeSig(idx: Int): CommitteeMemberSignature =
    CommitteeMemberSignature(PeerId(Hex(f"${idx}%02x" * 64)), Hex("aa" * 80), Hex("bb" * 64), Hex("cc" * 128), 0)

  private def mkCheckpoint(attested: Hash): ShardCheckpoint = {
    val bin: Signed[StateChannelSnapshotBinary] =
      Signed(
        StateChannelSnapshotBinary(Hash.empty, Array.emptyByteArray, SnapshotFee.MinValue),
        NonEmptySet.of(SignatureProof(io.constellationnetwork.schema.ID.Id(Hex("11" * 64)), Signature(Hex("22" * 70))))
      )
    val delta = ShardDerivedStateDelta.empty.copy(
      perMetagraphMptRoots = SortedMap(mgA -> attested),
      includedSnapshots = SortedMap(mgA -> NonEmptyList.of(bin))
    )
    ShardCheckpoint(
      shardId = ShardId(0).get,
      parentCheckpointHash = Hash("0" * 64),
      shardOrdinal = ShardOrdinal(1L),
      gl0AnchorOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(100L)),
      slot = Slot.unsafeApply(1L),
      derivedStateDelta = delta,
      emittedReceipts = List.empty,
      committeeSignatures = NonEmptyList.of(committeeSig(1)),
      epoch = EtaPeriod(0L)
    )
  }

  /** Acceptance-manager stub: only `watchtowerReExec` is exercised by the emitter. */
  private def stubManager(mismatches: List[WatchtowerMismatch]): ShardCheckpointGl0AcceptanceManager[IO] =
    new ShardCheckpointGl0AcceptanceManager[IO] {
      def evaluate(checkpoint: ShardCheckpoint): IO[ShardCheckpointAcceptResult] =
        IO.raiseError(new UnsupportedOperationException("not under test"))
      def verifyEmbedded(checkpoint: ShardCheckpoint): IO[ShardCheckpointAcceptResult] =
        IO.raiseError(new UnsupportedOperationException("not under test"))
      def watchtowerReExec(checkpoint: ShardCheckpoint): IO[List[WatchtowerMismatch]] = IO.pure(mismatches)
      def noteAdopted(shardId: ShardId, shardOrdinal: ShardOrdinal, checkpointHash: Hash): IO[Unit] = IO.unit
      def lastAdoptedOrd(shardId: ShardId): IO[Option[ShardOrdinal]] = IO.pure(None)
      def lastAdoptedAnchor(shardId: ShardId): IO[Option[Hash]] = IO.pure(None)
    }

  /** Sidecar-client stub: `publishFraudProof` fails the first `failFirst` calls (raised error) and `notOkFirst` further calls (ok=false),
    * then succeeds, capturing every wire it was handed. All other methods are inert.
    */
  private def recordingClient(
    calls: Ref[IO, Int],
    wires: Ref[IO, List[FraudProofEnvelopeWire]],
    failFirst: Int,
    notOkFirst: Int = 0
  ): SidecarClient.SidecarClientAlgebra[IO] =
    new SidecarClient.SidecarClientAlgebra[IO] {
      def publishSnapshot(msg: Snapshot) = IO.pure(PublishResponse(ok = true))
      def publishAttestation(msg: TipAttestation) = IO.pure(PublishResponse(ok = true))
      def publishRumor(msg: Rumor) = IO.pure(PublishResponse(ok = true))
      def publishMetagraphBinary(msg: MetagraphBinary) = IO.pure(PublishResponse(ok = true))
      def publishMetagraphAttestation(msg: MetagraphAttestation) = IO.pure(PublishResponse(ok = true))
      def publishAllowSpendBlock(payload: Array[Byte]) = IO.pure(PublishResponse(ok = true))
      def publishDAGBlock(payload: Array[Byte]) = IO.pure(PublishResponse(ok = true))
      def publishTokenLockBlock(payload: Array[Byte]) = IO.pure(PublishResponse(ok = true))
      def publishShardCheckpoint(msg: ShardCheckpointWire) = IO.pure(PublishResponse(ok = true))
      def publishShardCheckpointAttestation(msg: ShardCheckpointAttestationWire) = IO.pure(PublishResponse(ok = true))
      def publishFraudProof(msg: FraudProofEnvelopeWire): IO[PublishResponse] =
        calls.updateAndGet(_ + 1).flatMap { n =>
          if (n <= failFirst) IO.raiseError(new RuntimeException(s"UNAVAILABLE: sidecar restarting (attempt $n)"))
          else if (n <= failFirst + notOkFirst) IO.pure(PublishResponse(ok = false, error = s"topic not joined (attempt $n)"))
          else wires.update(msg :: _).as(PublishResponse(ok = true))
        }
      def confirmFinalized(topic: String, msgIds: List[Array[Byte]]) = IO.pure(ConfirmFinalizedResponse(dropped = 0))
      def health = IO.pure(HealthResponse(healthy = true))
      def peers = IO.pure(PeerCountResponse(total = 0))
      def channel: ManagedChannel = throw new UnsupportedOperationException("stub")
    }

  private def mkEmitter(
    kp: java.security.KeyPair,
    manager: ShardCheckpointGl0AcceptanceManager[IO],
    client: SidecarClient.SidecarClientAlgebra[IO],
    attempts: Int = 3
  )(implicit h: Hasher[IO], sp: SecurityProvider[IO]): WatchtowerFraudProofEmitter[IO] =
    WatchtowerFraudProofEmitter.make[IO](
      selfPeerId = PeerId.fromPublic(kp.getPublic),
      selfKeyPair = kp,
      acceptanceManager = manager,
      sidecarClient = client,
      publishAttempts = attempts,
      publishRetryDelay = 10.millis
    )

  private val mismatch = WatchtowerMismatch(mgA, attestedRoot, honestRoot)

  // ─── emit → publish → receive → admit (the full JVM loop) ──────────

  test("emit -> publish -> fromWire -> verdict UPHELD -> pool admit: the emitter's output is admissible slashing evidence") {
    case (h0, sp0) =>
      implicit val h: Hasher[IO] = h0
      implicit val sp: SecurityProvider[IO] = sp0
      val cp = mkCheckpoint(attestedRoot)
      for {
        kp <- KeyPairGenerator.makeKeyPair[IO]
        calls <- Ref.of[IO, Int](0)
        wires <- Ref.of[IO, List[FraudProofEnvelopeWire]](Nil)
        _ <- mkEmitter(kp, stubManager(List(mismatch)), recordingClient(calls, wires, failFirst = 0)).emit(cp)
        wire <- wires.get.map(_.headOption).flatMap(IO.fromOption(_)(new RuntimeException("no fraud proof published")))
        cpHash <- h.hash(cp.signingPreimage)

        // ── receive side: exactly what NakamotoSyncDaemon.handleFraudProof does with the wire ──
        fp <- FraudProofWireCodecs.fromWire[IO](wire)
        evidence = InvalidStateProofEvidence(
          shardId = fp.shardId,
          disputedCheckpoint = cp,
          metagraphAddress = fp.metagraphAddress,
          attestedRoot = cp.derivedStateDelta.perMetagraphMptRoots.getOrElse(fp.metagraphAddress, Hash.empty),
          fraudProof = fp
        )
        // REAL validator: re-derives (stub returns the honest root != attested => committee deviated),
        // checks the envelope header binding AND the emitter's REAL Ed25519 challenger signature.
        validator = InvalidStateProofValidator.make[IO](
          reDerivePerMgRoot = (_, _, _, _) => IO.pure(honestRoot),
          slashedReader = InvalidStateProofSlashedReader.neverSlashed[IO]
        )
        verdict <- validator.validate(evidence)
        pool <- WatchtowerFraudProofPool.make[IO]()
        _ <- verdict.traverse(pool.offer)
        staged <- pool.peekAll
      } yield
        expect(fp.disputedCheckpointHash === cpHash)
          .and(expect(fp.metagraphAddress === mgA))
          .and(expect(fp.claimedDerivation === attestedRoot))
          .and(expect(fp.challengerDerivation === honestRoot))
          .and(expect(fp.submitterId === PeerId.fromPublic(kp.getPublic)))
          .and(expect(verdict.isRight))
          .and(expect.same(1, staged.size))
          .and(expect(staged.headOption.exists(_.fraudProof.disputedCheckpointHash === cpHash)))
  }

  test("no mismatch => no publish (the honest-checkpoint common case is a no-op)") {
    case (h0, sp0) =>
      implicit val h: Hasher[IO] = h0
      implicit val sp: SecurityProvider[IO] = sp0
      for {
        kp <- KeyPairGenerator.makeKeyPair[IO]
        calls <- Ref.of[IO, Int](0)
        wires <- Ref.of[IO, List[FraudProofEnvelopeWire]](Nil)
        _ <- mkEmitter(kp, stubManager(Nil), recordingClient(calls, wires, failFirst = 0)).emit(mkCheckpoint(attestedRoot))
        n <- calls.get
      } yield expect.same(0, n)
  }

  // ─── retry-or-outbox (M4): the local hop must survive a transient failure ───

  test("a transiently-failing publish is retried until it lands (evidence is not dropped on the first local hiccup)") {
    case (h0, sp0) =>
      implicit val h: Hasher[IO] = h0
      implicit val sp: SecurityProvider[IO] = sp0
      for {
        kp <- KeyPairGenerator.makeKeyPair[IO]
        calls <- Ref.of[IO, Int](0)
        wires <- Ref.of[IO, List[FraudProofEnvelopeWire]](Nil)
        _ <- mkEmitter(kp, stubManager(List(mismatch)), recordingClient(calls, wires, failFirst = 1)).emit(mkCheckpoint(attestedRoot))
        n <- calls.get
        published <- wires.get
      } yield expect.same(2, n).and(expect.same(1, published.size))
  }

  test("an ok=false sidecar response is retried too (either failure shape means the evidence has not left the node)") {
    case (h0, sp0) =>
      implicit val h: Hasher[IO] = h0
      implicit val sp: SecurityProvider[IO] = sp0
      for {
        kp <- KeyPairGenerator.makeKeyPair[IO]
        calls <- Ref.of[IO, Int](0)
        wires <- Ref.of[IO, List[FraudProofEnvelopeWire]](Nil)
        _ <- mkEmitter(kp, stubManager(List(mismatch)), recordingClient(calls, wires, failFirst = 0, notOkFirst = 2))
          .emit(mkCheckpoint(attestedRoot))
        n <- calls.get
        published <- wires.get
      } yield expect.same(3, n).and(expect.same(1, published.size))
  }

  test("total publish failure is swallowed after exactly the configured attempts (the receive path must never crash)") {
    case (h0, sp0) =>
      implicit val h: Hasher[IO] = h0
      implicit val sp: SecurityProvider[IO] = sp0
      for {
        kp <- KeyPairGenerator.makeKeyPair[IO]
        calls <- Ref.of[IO, Int](0)
        wires <- Ref.of[IO, List[FraudProofEnvelopeWire]](Nil)
        outcome <- mkEmitter(kp, stubManager(List(mismatch)), recordingClient(calls, wires, failFirst = 99), attempts = 3)
          .emit(mkCheckpoint(attestedRoot))
          .attempt
        n <- calls.get
      } yield expect(outcome.isRight).and(expect.same(3, n))
  }
}
