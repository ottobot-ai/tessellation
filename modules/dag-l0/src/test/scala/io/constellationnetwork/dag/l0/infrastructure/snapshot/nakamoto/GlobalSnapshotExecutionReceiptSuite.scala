package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.effect.{IO, Ref}
import cats.syntax.all._

import scala.reflect.runtime.universe.runtimeMirror

import io.constellationnetwork.dag.l0.infrastructure.snapshot._
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto._
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.MptOverlay
import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.schema.nakamoto.LddConfig
import io.constellationnetwork.schema.nakamoto.slot._
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.signature.Signed

import eu.timepit.refined.types.numeric.NonNegLong
import shapeless.test.illTyped
import weaver.SimpleIOSuite

object GlobalSnapshotExecutionReceiptSuite extends SimpleIOSuite {

  test("Scala callers cannot directly construct or decode global execution receipts") {
    illTyped("""new GlobalSnapshotReplayReceipt {}""")
    illTyped("""new GlobalSnapshotProposalExecutionReceipt {}""")
    illTyped("""GlobalSnapshotReplayReceipt.apply(null, null)""")
    illTyped("""null.asInstanceOf[GlobalSnapshotReplayReceipt].copy()""")
    illTyped("""GlobalSnapshotProposalExecutionReceipt.apply(null, null, null)""")
    illTyped("""null.asInstanceOf[GlobalSnapshotProposalExecutionReceipt].copy()""")
    illTyped("""NakamotoSnapshotValidator.Valid(null, null)""")
    illTyped("""implicitly[io.circe.Decoder[GlobalSnapshotReplayReceipt]]""")
    illTyped("""implicitly[scodec.Codec[GlobalSnapshotReplayReceipt]]""")
    illTyped("""implicitly[io.circe.Decoder[GlobalSnapshotProposalExecutionReceipt]]""")
    illTyped("""implicitly[scodec.Codec[GlobalSnapshotProposalExecutionReceipt]]""")

    IO.pure(expect(true))
  }

  test("reflected replay-valid results with null or wrong issuer invoke zero effects") {
    def forgedLowerReceipt(issuer: AnyRef): GlobalSnapshotReplayReceipt = {
      val clazz = Class.forName(
        "io.constellationnetwork.dag.l0.infrastructure.snapshot.GlobalSnapshotConsensusFunctions$ReplayReceipt"
      )
      clazz.getDeclaredConstructors.head
        .newInstance(null, null, issuer)
        .asInstanceOf[GlobalSnapshotReplayReceipt]
    }

    def forgedValidationResult(issuer: AnyRef): NakamotoSnapshotValidator.ValidationResult = {
      val clazz = Class.forName(
        "io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.NakamotoSnapshotValidator$ReplayValidResult"
      )
      clazz.getDeclaredConstructors.head
        .newInstance(null, null, forgedLowerReceipt(new Object), issuer)
        .asInstanceOf[NakamotoSnapshotValidator.ValidationResult]
    }

    for {
      effects <- Ref.of[IO, Int](0)
      consumed <- List(
        null.asInstanceOf[NakamotoSnapshotValidator.ValidationResult],
        forgedValidationResult(null),
        forgedValidationResult(new Object)
      ).traverse(result => NakamotoSnapshotValidator.consumeReplayValidated[IO](result)((_, _) => effects.update(_ + 1)))
      observed <- effects.get
    } yield expect.all(consumed.forall(result => !result), observed == 0)
  }

  test("full snapshot validation mints one replay result carrying the exact snapshot and replayed context") {
    SecurityProvider.forAsync[IO].use { implicit securityProvider =>
      JsonSerializer.forAsync[IO].flatMap { implicit jsonSerializer =>
        Metrics.forAsync[IO](Seq.empty).use { implicit metrics =>
          CanonicalOperatorConsensusFixture.make.use { operator =>
            implicit val hasher: Hasher[IO] = Hasher.forJson[IO]
            implicit val hasherSelector: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(hasher)
            implicit val stateProofSelector: GlobalStateProofSelector =
              GlobalStateProofSelector(SnapshotOrdinal(NonNegLong.unsafeFrom(Long.MaxValue)))
            implicit val withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit =
              io.constellationnetwork.schema.mpt.WithdrawalTimeLimit.none

            val slot = Slot(NonNegLong.unsafeFrom(1L))
            val etaBytes = Array.fill(32)(0x41.toByte)
            val etaHash = Hash(Hex.fromBytes(etaBytes).value)
            val eligibilityChecker = new EligibilityChecker[IO](null, null)
            val lddConfig = LddConfig(1, 0, Ratio.One, Ratio.One)

            for {
              producer <- GlobalSnapshotConsensusFunctionsSuite.mkGlobalSnapshotConsensusFunctions()
              follower <- GlobalSnapshotConsensusFunctionsSuite.mkGlobalSnapshotConsensusFunctions()
              producerKeyPair = operator.localLongTermKeyPairForConsensusTest
              producerId = operator.resolvedPair.operatorPeerId
              vrfPublicKeyBytes = operator.resolvedPair.vrfPublicKey.toBytes
              vrfMaterial <- eligibilityChecker
                .checkEligibility(operator.localVrfSecret, slot, 2L, etaBytes, Ratio.One, lddConfig)
                .flatMap(
                  IO.fromOption(_)(
                    new IllegalStateException("canonical registered operator was not eligible in an always-eligible test slot")
                  )
                )
              (vrfProofBytes, vrfOutputBytes) = vrfMaterial
              genesis = GlobalSnapshot.mkGenesis(Map.empty, EpochProgress.MinValue)
              signedGenesis <- Signed.forAsyncHasher[IO, GlobalSnapshot](genesis, producerKeyPair)
              lastArtifact <- GlobalIncrementalSnapshot.fromGlobalSnapshot[IO](signedGenesis.value)
              signedLastArtifact <- Signed.forAsyncHasher[IO, GlobalIncrementalSnapshot](lastArtifact, producerKeyPair)
              producerReceipt <- producer.createProposalArtifactWithExecutionReceipt(
                SnapshotOrdinal.MinValue,
                signedLastArtifact,
                signedGenesis.value.info.toGlobalSnapshotInfo,
                hasher,
                io.constellationnetwork.node.shared.infrastructure.consensus.trigger.EventTrigger,
                Set.empty,
                Set(producerId),
                _ => none.pure[IO]
              )
              producerExecution <- IO.fromOption(producer.consumeProposalExecutionReceipt(producerReceipt))(
                new IllegalStateException("producer rejected its execution receipt")
              )
              (artifact, expectedContext, _) = producerExecution
              certificate = SlotCertificate(
                slot = slot,
                parentSlot = Slot.MinValue,
                vrfProof = VrfProof(Hex.fromBytes(vrfProofBytes)),
                vrfOutput = VrfOutput(Hex.fromBytes(vrfOutputBytes)),
                vrfPublicKey = VrfPublicKey.fromBytes(vrfPublicKeyBytes),
                eta = etaHash,
                activePoolSize = 1,
                activePoolHash = Hash.empty,
                subchainLevelCounts = SlotCertificate.ZeroSubchainLevelCounts
              )
              signedSnapshot <- Signed.forAsyncHasher[IO, GlobalIncrementalSnapshot](
                artifact.copy(slotCertificate = certificate.some, eta = etaHash.some),
                producerKeyPair
              )
              stakeRegistry <- StakeRegistry.equalWeight[IO]
              _ <- stakeRegistry.updateValidators(Set(producerId))
              mptProducer <- InMemoryMerklePatriciaProducer.make[IO]()
              mptStore <- MptStore.make[IO, GlobalStateKey](mptProducer, GlobalStateKey.toHex[IO])
              parentChildTree <- ParentChildTree.make[IO]
              outerOverlay = MptOverlay.passthrough[IO, GlobalStateKey](mptStore, parentChildTree)
              pendingAccumulators <- Ref.of[
                IO,
                Map[
                  Hash,
                  (SnapshotOrdinal, io.constellationnetwork.schema.mpt.GlobalStateConverter.StateChangesAccumulator)
                ]
              ](Map.empty)
              pendingPostBytes <- Ref.of[IO, Map[Hash, (SnapshotOrdinal, Map[Hex, Array[Byte]])]](Map.empty)
              result <- NakamotoSnapshotValidator.validate[IO](
                signedSnapshot = signedSnapshot,
                slot = slot.value.value,
                vrfProof = vrfProofBytes,
                vrfPublicKey = vrfPublicKeyBytes,
                producerIdBytes = producerId.value.toBytes,
                eta = etaBytes,
                slotGap = 2L,
                etaRotationSnapshots = 2550L,
                stakeRegistry = stakeRegistry,
                operatorKeys = operator.resolvedPair,
                lddConfig = lddConfig,
                eligibilityChecker = eligibilityChecker,
                consensusFns = follower,
                lastSignedArtifact = signedLastArtifact,
                lastContext = signedGenesis.value.info.toGlobalSnapshotInfo,
                getByOrdinal = _ => none.pure[IO],
                mptOverlay = outerOverlay,
                pendingAccumulatorsRef = pendingAccumulators,
                pendingPostBytesRef = pendingPostBytes
              )
              callbacks <- Ref.of[IO, List[(Signed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)]](List.empty)
              consumed <- NakamotoSnapshotValidator.consumeReplayValidated[IO](result) { (snapshot, context) =>
                callbacks.update(_ :+ (snapshot -> context))
              }
              observed <- callbacks.get
            } yield expect.all(consumed, observed == List(signedSnapshot -> expectedContext))
          }
        }
      }
    }
  }

  test("the consensus-functions receipt factory is Scala-sealed inside the trusted validator process") {
    val consensusFunctionsClass = classOf[GlobalSnapshotConsensusFunctions[IO]]
    val isSealed = runtimeMirror(consensusFunctionsClass.getClassLoader).classSymbol(consensusFunctionsClass).isSealed

    IO.pure(expect(isSealed))
  }
}
