package io.constellationnetwork.dag.l0.infrastructure.snapshot

import java.security.KeyPair

import cats.data.NonEmptyList
import cats.effect.{IO, Resource}
import cats.syntax.all._

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.infrastructure.sharding.RegisteredCheckpointSigner
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nakamoto.slot.{Slot => SlotT}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}

import eu.timepit.refined.types.all.NonNegLong
import weaver.MutableIOSuite

/** Slice 14 — unit tests for [[GlobalSnapshotConsensusFunctions.spliceCommitteeSignatures]]: the pure helper the gl0 consensus LEADER uses
  * to enrich a candidate checkpoint's `committeeSignatures` with the committee attestations collected in the per-shard `ShardTipTracker`.
  * The count affects producer-side selection only; `ShardCheckpointGl0AcceptanceManager.verifyEmbedded` independently verifies the
  * execution certificate, continuity, namespace-bounded diff, and resulting root before adoption.
  *
  * Determinism is the load-bearing property: every follower threads the leader's enriched set unchanged and re-verifies it, so the splice
  * must (a) leave the canonical signing-preimage UNTOUCHED (`committeeSignatures` is excluded from `ShardCheckpointSigPreimage`), (b) dedup
  * by `peerId` so the producer's own embedded signature is never double-counted, and (c) order the appended signers canonically (sorted by
  * `peerId` hex) so the leader re-validating its own artifact yields byte-identical bytes regardless of `Map` iteration order.
  */
object SpliceCommitteeSignaturesSuite extends MutableIOSuite {

  final case class RegisteredOperator(keyPair: KeyPair, peerId: PeerId)

  final case class TestContext(
    hasher: Hasher[IO],
    securityProvider: SecurityProvider[IO],
    checkpointSigner: RegisteredCheckpointSigner,
    operators: List[RegisteredOperator]
  )

  override type Res = TestContext

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(securityProvider: SecurityProvider[IO]) = sp
      implicit0(jsonSerializer: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(hasher: Hasher[IO]) = Hasher.forJson[IO]
      checkpointSigner <- RegisteredCheckpointSigner.make.asResource
      keyPairs <- Resource.eval(List.fill(4)(KeyPairGenerator.makeKeyPair[IO]).sequence)
      operators = keyPairs
        .map(keyPair => RegisteredOperator(keyPair, PeerId.fromPublic(keyPair.getPublic)))
        .sortBy(_.peerId.value.value)
      _ <- Resource.eval(operators.traverse_(operator => checkpointSigner.preregisterGenesis(operator.keyPair, operator.peerId)))
    } yield TestContext(hasher, securityProvider, checkpointSigner, operators)

  /** Construction-only placeholder required by the non-empty wire type. It is replaced with a registered signature before any checkpoint is
    * returned, spliced, or treated as a valid artifact; committee signatures are excluded from the signing preimage.
    */
  private def constructionScaffold(p: PeerId): CommitteeMemberSignature =
    CommitteeMemberSignature(
      peerId = p,
      vrfProof = Hex.fromBytes(Array.emptyByteArray),
      ed25519Sig = Hex.fromBytes(Array.emptyByteArray),
      kesProductSig = Hex.fromBytes(Array.emptyByteArray),
      kesTreeStep = 0
    )

  private def checkpointTemplate(producer: PeerId): ShardCheckpoint =
    ShardCheckpoint(
      shardId = ShardId.unsafeApply(0),
      parentCheckpointHash = Hash("0" * 64),
      shardOrdinal = ShardOrdinal(1L),
      gl0AnchorOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(5L)),
      slot = SlotT.unsafeApply(5L),
      derivedStateDelta = ShardDerivedStateDelta.empty,
      committeeSignatures = NonEmptyList.of(constructionScaffold(producer)),
      epoch = EtaPeriod(0L)
    )

  private def sign(
    checkpoint: ShardCheckpoint,
    operator: RegisteredOperator,
    ctx: TestContext
  ): IO[CommitteeMemberSignature] = {
    implicit val hasher: Hasher[IO] = ctx.hasher
    implicit val securityProvider: SecurityProvider[IO] = ctx.securityProvider

    ctx.checkpointSigner.sign(checkpoint, operator.keyPair, operator.peerId)
  }

  private def checkpointWithProducer(operator: RegisteredOperator, ctx: TestContext): IO[ShardCheckpoint] = {
    val template = checkpointTemplate(operator.peerId)
    sign(template, operator, ctx).map(signature => template.copy(committeeSignatures = NonEmptyList.one(signature)))
  }

  test("enriches with registered collected signatures, dedups the producer, and sorts appended signers") { ctx =>
    val List(pA, pB, pC, pD) = ctx.operators: @unchecked

    for {
      checkpoint <- checkpointWithProducer(pB, ctx)
      signatures <- List(pD, pB, pA, pC).traverse(operator => sign(checkpoint, operator, ctx).map(operator.peerId -> _))
      collected = signatures.toMap
      out = GlobalSnapshotConsensusFunctions.spliceCommitteeSignatures(checkpoint, collected)
      peers = out.committeeSignatures.toList.map(_.peerId)
    } yield
      expect(out.committeeSignatures.size == 4)
        .and(expect(peers.toSet == ctx.operators.map(_.peerId).toSet))
        .and(expect(peers.head == pB.peerId))
        .and(expect(peers.tail == List(pA.peerId, pC.peerId, pD.peerId)))
  }

  test("leaves the signing preimage (canonical checkpoint-hash bytes) unchanged") { ctx =>
    val List(pA, pB, pC, _) = ctx.operators: @unchecked

    for {
      checkpoint <- checkpointWithProducer(pB, ctx)
      a <- sign(checkpoint, pA, ctx)
      c <- sign(checkpoint, pC, ctx)
      out = GlobalSnapshotConsensusFunctions.spliceCommitteeSignatures(checkpoint, Map(pA.peerId -> a, pC.peerId -> c))
    } yield expect(out.signingPreimage == checkpoint.signingPreimage)
  }

  test("wire signature reordering changes the nominal producer label without changing signed preimage") { ctx =>
    val List(pA, pB, _, _) = ctx.operators: @unchecked

    for {
      checkpoint <- checkpointWithProducer(pB, ctx)
      a <- sign(checkpoint, pA, ctx)
      reordered = checkpoint.copy(committeeSignatures = NonEmptyList(a, List(checkpoint.committeeSignatures.head)))
    } yield
      expect(checkpoint.producerSignature.peerId == pB.peerId)
        .and(expect(reordered.producerSignature.peerId == pA.peerId))
        .and(expect(reordered.signingPreimage == checkpoint.signingPreimage))
  }

  test("no-op when collected is empty") { ctx =>
    checkpointWithProducer(ctx.operators(1), ctx).map { checkpoint =>
      expect(GlobalSnapshotConsensusFunctions.spliceCommitteeSignatures(checkpoint, Map.empty) == checkpoint)
    }
  }

  test("no-op when the only collected signer is the already-embedded producer (dedup)") { ctx =>
    checkpointWithProducer(ctx.operators(1), ctx).map { checkpoint =>
      val producer = checkpoint.producerSignature
      expect(
        GlobalSnapshotConsensusFunctions.spliceCommitteeSignatures(checkpoint, Map(producer.peerId -> producer)) == checkpoint
      )
    }
  }
}
