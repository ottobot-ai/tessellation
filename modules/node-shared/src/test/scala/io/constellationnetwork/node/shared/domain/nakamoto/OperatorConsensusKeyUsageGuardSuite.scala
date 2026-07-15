package io.constellationnetwork.node.shared.domain.nakamoto

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import cats.effect.IO

import scala.jdk.CollectionConverters._
import scala.util.matching.Regex

import weaver.SimpleIOSuite

/** Source-inventory tripwire for the atomic operator KES+VRF identity boundary.
  *
  * This is deliberately a reviewed allowlist, not a claim that textual scanning proves consensus correctness. A new raw VRF/KES primitive
  * consumer, KES lifecycle/sign/verify reference, or direct atomic-record fixture must fail this suite and receive an explicit
  * registry/history review before its path is admitted here.
  */
object OperatorConsensusKeyUsageGuardSuite extends SimpleIOSuite {

  private final case class Source(path: String, contents: String)

  private val reviewedRawVrfConsumers: Map[String, String] = Map(
    "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/LocalOperatorKeyPairGate.scala" ->
      "compares local KES and derived VRF secrets with one resolved atomic pair",
    "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/NakamotoSyncDaemon.scala" ->
      "verifies received snapshot eligibility under the resolved operator pair",
    "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/SnapshotLeaderLoop.scala" ->
      "derives the local VRF secret and gates signing through LocalOperatorKeyPairGate",
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/CommitteeSortition.scala" ->
      "implements admission VRF proof production and verification; callers resolve the pair",
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/EligibilityChecker.scala" ->
      "implements leader VRF proof production and verification; callers resolve the pair",
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/nipopow/TowerVerifier.scala" ->
      "verifies every carried proof/output only after atomic-pair resolution; historical eligibility remains fail-closed",
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/slashing/ShardCheckpointEquivocationValidator.scala" ->
      "verifies evidence only after historical atomic-pair resolution",
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/sharding/ShardCheckpointChainStoreRecovery.scala" ->
      "converts an already validated checkpoint proof to its stored VRF output",
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/ShardCheckpointGl0AcceptanceManager.scala" ->
      "verifies checkpoint proofs under resolved atomic operator pairs",
    "modules/tools/src/main/scala/io/constellationnetwork/tools/genesis/GenesisGenerator.scala" ->
      "derives the VRF half of a complete signed genesis operator pair"
  )

  private val rawVrfPrimitiveImplementations: Set[String] = Set(
    "modules/shared/src/main/scala/io/constellationnetwork/security/vrf/EcVrf25519.scala",
    "modules/shared/src/main/scala/io/constellationnetwork/security/vrf/VrfKeyDeriver.scala"
  )

  // Populated with exact comment-stripped occurrence counts. Keeping counts separate from the
  // rationale maps makes an approved source path a bounded exception rather than an escape hatch.
  private val reviewedRawVrfProductionCounts: Map[String, Int] = Map(
    "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/LocalOperatorKeyPairGate.scala" -> 3,
    "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/NakamotoSyncDaemon.scala" -> 2,
    "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/SnapshotLeaderLoop.scala" -> 2,
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/CommitteeSortition.scala" -> 2,
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/EligibilityChecker.scala" -> 2,
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/nipopow/TowerVerifier.scala" -> 2,
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/slashing/ShardCheckpointEquivocationValidator.scala" -> 2,
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/sharding/ShardCheckpointChainStoreRecovery.scala" -> 2,
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/ShardCheckpointGl0AcceptanceManager.scala" -> 2,
    "modules/tools/src/main/scala/io/constellationnetwork/tools/genesis/GenesisGenerator.scala" -> 2
  )

  /** Exact reviewed inventory of test sources that name a raw VRF primitive after comment stripping. These categories distinguish tests
    * where arbitrary key material is the subject from consensus fixtures where the raw primitive is permitted only after constructing a
    * complete canonical operator identity. String literals remain searchable, so this guard intentionally inventories itself.
    */
  private val reviewedRawVrfTestConsumerCategories: List[(String, Map[String, String])] = List(
    "isolated crypto primitive" -> Map(
      "modules/shared/src/test/scala/io/constellationnetwork/security/vrf/EcVrf25519Suite.scala" ->
        "isolated proof, verification, and deterministic-vector coverage for the primitive itself",
      "modules/shared/src/test/scala/io/constellationnetwork/security/vrf/EcVrf25519UniformitySuite.scala" ->
        "isolated statistical output-distribution coverage for the primitive itself",
      "modules/shared/src/test/scala/io/constellationnetwork/security/vrf/VrfKeyDeriverSuite.scala" ->
        "isolated deterministic long-term-key to VRF-key derivation coverage",
      "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/domain/nakamoto/OperatorConsensusKeyUsageGuardSuite.scala" ->
        "inventory tripwire self-match from reviewed primitive path string literals; it performs no VRF operation"
    ),
    "signed-genesis fixture/loader" -> Map(
      "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/domain/nakamoto/CanonicalOperatorConsensusFixture.scala" ->
        "derives the VRF half before long-term signing, loader validation, and rooted atomic-pair rematerialization",
      "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/infrastructure/genesis/GenesisOperatorKeyCommitmentSuite.scala" ->
        "constructs a complete signed genesis operator commitment and verifies its rooted key fields",
      "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/infrastructure/genesis/KesRegistryLoaderSuite.scala" ->
        "derives the VRF half of complete signed genesis records while testing the atomic loader projection",
      "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/infrastructure/genesis/L0GenesisStateProofConsistencySuite.scala" ->
        "derives a complete signed genesis pair whose registry commitment is checked against the genesis state proof",
      "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/infrastructure/genesis/VrfRegistryLoaderSuite.scala" ->
        "derives expected VRF bytes while testing loader validation of complete signed genesis pairs"
    ),
    "registered high-level consensus" -> Map(
      "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/domain/nakamoto/MetagraphCommitteeGateSuite.scala" ->
        "runs admission-gate orchestration from a canonical registered operator; raw VK derivation is confined to explicit test inputs",
      "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/domain/nakamoto/nipopow/TowerProofBuilderSuite.scala" ->
        "builds tower certificates with the secret matching a loader-validated canonical genesis pair",
      "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/domain/nakamoto/sharding/ShardSlotLeaderSuite.scala" ->
        "checks shard possession proofs against the canonical fixture's registered pair",
      "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/infrastructure/sharding/RegisteredCheckpointSigner.scala" ->
        "test signer derives, registers, resolves, and compares a complete genesis pair before emitting any VRF proof",
      "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/infrastructure/sharding/RegisteredCheckpointSignerSuite.scala" ->
        "compares derived bytes with the signer fixture's loader-validated atomic registration",
      "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/infrastructure/sharding/ShardCheckpointAttestationEmitterSuite.scala" ->
        "passes local secrets only with the matching preregistered atomic checkpoint-signer identity",
      "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/ShardCheckpointGl0AcceptanceManagerSuite.scala" ->
        "constructs registered checkpoint proofs for high-level GL0 acceptance and explicit mismatch rejection"
    ),
    "explicit attacker/negative evidence" -> Map(
      "modules/dag-l0/src/test/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/CatchUpVerificationSuite.scala" ->
        "constructs a cryptographically coherent transport envelope so metadata mutations can be rejected independently",
      "modules/dag-l0/src/test/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/LocalOperatorKeyPairGateSuite.scala" ->
        "derives a substituted key specifically to prove local material cannot replace the preregistered pair",
      "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/domain/nakamoto/nipopow/TowerVerifierSuite.scala" ->
        "starts from a registered proof/output pair, then forges keys, proofs, and outputs to verify fail-closed rejection",
      "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/domain/nakamoto/slashing/ShardCheckpointEquivocationValidatorSuite.scala" ->
        "constructs registered equivocation evidence plus wrong-key and malformed-proof attacker cases"
    )
  )

  private val reviewedRawVrfTestCounts: Map[String, Int] = Map(
    "modules/dag-l0/src/test/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/CatchUpVerificationSuite.scala" -> 2,
    "modules/dag-l0/src/test/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/LocalOperatorKeyPairGateSuite.scala" -> 2,
    "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/domain/nakamoto/CanonicalOperatorConsensusFixture.scala" -> 2,
    "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/domain/nakamoto/MetagraphCommitteeGateSuite.scala" -> 1,
    "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/domain/nakamoto/OperatorConsensusKeyUsageGuardSuite.scala" -> 2,
    "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/domain/nakamoto/nipopow/TowerProofBuilderSuite.scala" -> 2,
    "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/domain/nakamoto/nipopow/TowerVerifierSuite.scala" -> 3,
    "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/domain/nakamoto/sharding/ShardSlotLeaderSuite.scala" -> 2,
    "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/domain/nakamoto/slashing/ShardCheckpointEquivocationValidatorSuite.scala" -> 3,
    "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/infrastructure/genesis/GenesisOperatorKeyCommitmentSuite.scala" -> 2,
    "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/infrastructure/genesis/KesRegistryLoaderSuite.scala" -> 2,
    "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/infrastructure/genesis/L0GenesisStateProofConsistencySuite.scala" -> 2,
    "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/infrastructure/genesis/VrfRegistryLoaderSuite.scala" -> 3,
    "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/infrastructure/sharding/RegisteredCheckpointSigner.scala" -> 5,
    "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/infrastructure/sharding/RegisteredCheckpointSignerSuite.scala" -> 2,
    "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/infrastructure/sharding/ShardCheckpointAttestationEmitterSuite.scala" -> 6,
    "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/ShardCheckpointGl0AcceptanceManagerSuite.scala" -> 4,
    "modules/shared/src/test/scala/io/constellationnetwork/security/vrf/EcVrf25519Suite.scala" -> 1,
    "modules/shared/src/test/scala/io/constellationnetwork/security/vrf/EcVrf25519UniformitySuite.scala" -> 1,
    "modules/shared/src/test/scala/io/constellationnetwork/security/vrf/VrfKeyDeriverSuite.scala" -> 20
  )

  // Exact counts keep an allowed negative/algebra suite from becoming a general fixture escape hatch.
  private val reviewedDirectRecordFixtures: Map[String, (Int, String)] = Map(
    "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/domain/nakamoto/ActiveOperatorConsensusKeysSuite.scala" ->
      (2 -> "active-view registry algebra and explicit runtime-record rejection"),
    "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/domain/nakamoto/HistoricalOperatorConsensusKeyRegistrySuite.scala" ->
      (1 -> "branch-historical registry algebra"),
    "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/domain/nakamoto/OperatorConsensusKeyRegistrySuite.scala" ->
      (1 -> "atomic registry construction and malformed-record algebra"),
    "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/domain/nakamoto/kes/MutableKesRegistrySuite.scala" ->
      (1 -> "runtime-registration registry algebra rooted in a synthetic genesis registry"),
    "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/domain/nakamoto/nipopow/TowerVerifierSuite.scala" ->
      (1 -> "explicit negative case for a runtime pair rejected by the genesis-only resolver"),
    "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/infrastructure/sharding/ShardCheckpointWiringSuite.scala" ->
      (1 -> "explicit pre-activation runtime-pair rejection")
  )

  /** Exact production constructor inventory. A new materialization site bypasses provenance review unless this count changes explicitly. */
  private val reviewedProductionDirectRecordSites: Map[String, (Int, String)] = Map(
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/genesis/L0GenesisLoader.scala" ->
      (1 -> "materializes period-zero keys only from validated root-authenticated signed genesis records"),
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/kes/MutableKesRegistry.scala" ->
      (3 -> "case-class definition plus rooted runtime-record and validated current-genesis projections"),
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/HistoricalOperatorConsensusKeyRegistry.scala" ->
      (1 -> "materializes a runtime pair only from the validated exact branch-historical N-2 registry view")
  )

  private val reviewedKesProvisioningProduction: Map[String, (Int, String)] = Map(
    "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/LocalOperatorKeyPairGate.scala" ->
      (1 -> "opens provisioned KES material and authenticates it against the same resolved atomic pair before returning it"),
    "modules/tools/src/main/scala/io/constellationnetwork/tools/genesis/GenesisGenerator.scala" ->
      (1 -> "generates the KES half of a complete long-term-signed genesis operator pair")
  )

  private val reviewedKesProvisioningTests: Map[String, (Int, String)] = Map(
    "modules/dag-l0/src/test/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/KesGossipVerificationSuite.scala" ->
      (2 -> "creates deliberately unregistered KES signers only for replacement-key and invalid-signature rejection cases"),
    "modules/dag-l0/src/test/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/LocalOperatorKeyPairGateSuite.scala" ->
      (1 -> "provisions local material to test same-pair acceptance and mismatch rejection"),
    "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/domain/nakamoto/CanonicalOperatorConsensusFixture.scala" ->
      (2 -> "generates and opens KES material before long-term signing, loading, and resolving the complete genesis pair"),
    "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/domain/nakamoto/slashing/ShardCheckpointEquivocationValidatorSuite.scala" ->
      (1 -> "generates a different KES key only for atomic half-pair substitution rejection"),
    "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/domain/nakamoto/slashing/SlashableEvidenceValidatorSuite.scala" ->
      (1 -> "generates a different KES key only for atomic half-pair substitution rejection"),
    "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/infrastructure/sharding/RegisteredCheckpointSigner.scala" ->
      (1 -> "opens the KES material matching a loader-validated canonical genesis pair"),
    "modules/shared/src/test/scala/io/constellationnetwork/security/kes/OperationalKeyMakerSuite.scala" ->
      (13 -> "isolated KES lifecycle, persistence, bootstrap, and malformed-store primitive coverage")
  )

  private val reviewedKesSigningProduction: Map[String, (Int, String)] = Map(
    "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/GlobalSnapshotConsensus.scala" ->
      (1 -> "signs admission/checkpoint evidence only with a capability returned by the resolved-pair local gate"),
    "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/SnapshotLeaderLoop.scala" ->
      (1 -> "signs a produced snapshot only with the slot's preregistration-gated signing capability")
  )

  private val reviewedKesSigningTests: Map[String, (Int, String)] = Map(
    "modules/dag-l0/src/test/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/KesGossipVerificationSuite.scala" ->
      (3 -> "uses a canonical pair for positive cases and deliberately unregistered signers for rejection cases"),
    "modules/dag-l0/src/test/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/ShardCommitteeReExecutionSuite.scala" ->
      (1 -> "signs through the canonical registered operator fixture"),
    "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/domain/nakamoto/slashing/ShardCheckpointEquivocationValidatorSuite.scala" ->
      (1 -> "builds evidence with the canonical fixture's registered KES signer"),
    "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/domain/nakamoto/slashing/SlashableEvidenceValidatorSuite.scala" ->
      (1 -> "builds evidence with the canonical fixture's registered KES signer"),
    "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/domain/nakamoto/MetagraphCommitteeGateSuite.scala" ->
      (1 -> "signs high-level admission evidence with the loader-validated canonical operator fixture"),
    "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/infrastructure/sharding/RegisteredCheckpointSigner.scala" ->
      (1 -> "signs only after resolving and comparing the complete canonical pair"),
    "modules/shared/src/test/scala/io/constellationnetwork/security/kes/OperationalKeyMakerSuite.scala" ->
      (8 -> "isolated KES signing, evolution, persistence, and forward-security primitive coverage")
  )

  private val reviewedKesVerificationProduction: Map[String, (Int, String)] = Map(
    "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/KesGossipVerification.scala" ->
      (1 -> "derives the step from and verifies under the resolved complete active operator pair"),
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/slashing/ShardCheckpointEquivocationValidator.scala" ->
      (1 -> "verifies under an exact historical atomic-pair resolution or returns unverifiable"),
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/slashing/SlashableEvidenceValidator.scala" ->
      (1 -> "verifies under an exact historical atomic-pair resolution or returns unverifiable"),
    "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/ShardCheckpointGl0AcceptanceManager.scala" ->
      (1 -> "verifies the derived artifact-period step under the resolved complete operator pair")
  )

  private val reviewedKesVerificationTests: Map[String, (Int, String)] = Map(
    "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/domain/nakamoto/MetagraphCommitteeGateSuite.scala" ->
      (1 -> "verifies high-level admission evidence under the complete loader-validated genesis pair"),
    "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/infrastructure/sharding/ShardCheckpointProducerSuite.scala" ->
      (1 -> "verifies the producer happy path's KES evidence under its complete registered pair"),
    "modules/shared/src/test/scala/io/constellationnetwork/security/kes/SignatureCodecSuite.scala" ->
      (1 -> "isolated KES signature-codec roundtrip through the public verification wrapper")
  )

  private val reviewedKesPrimitiveProduction: Map[String, (Int, String)] = Map(
    "modules/shared/src/main/scala/io/constellationnetwork/security/kes/OperationalKeyMaker.scala" ->
      (4 -> "the sole lifecycle-managed production wrapper around package-private KES product operations")
  )

  private val reviewedKesPrimitiveTests: Map[String, (Int, String)] = Map(
    "modules/shared/src/test/scala/io/constellationnetwork/security/kes/KesProductSuite.scala" ->
      (9 -> "isolated product-composition primitive coverage"),
    "modules/shared/src/test/scala/io/constellationnetwork/security/kes/KesSumSuite.scala" ->
      (7 -> "isolated sum-composition primitive coverage"),
    "modules/shared/src/test/scala/io/constellationnetwork/security/kes/OperationalKeyMakerSuite.scala" ->
      (6 -> "isolated lifecycle wrapper coverage against the underlying product primitive"),
    "modules/shared/src/test/scala/io/constellationnetwork/security/kes/ReadOnceEnforcementSuite.scala" ->
      (4 -> "isolated secret-erasure and read-once primitive coverage"),
    "modules/shared/src/test/scala/io/constellationnetwork/security/kes/SecretKeyCodecSuite.scala" ->
      (2 -> "isolated KES secret-key codec coverage"),
    "modules/shared/src/test/scala/io/constellationnetwork/security/kes/SignatureCodecSuite.scala" ->
      (4 -> "isolated KES signature codec coverage")
  )

  private val rawVrfUse: Regex =
    ("\\b(?:Ec" + "Vrf25519|Vrf" + "KeyDeriver)\\b").r

  private val rawKesProvisioningUse: Regex =
    ("\\bOperational" + "KeyMaker\\s*\\.\\s*(?:make|bootstrap|generateFreshKesKeyMaterial)\\b").r

  private val rawKesSigningUse: Regex =
    ("\\bsign" + "At\\b").r

  private val rawKesVerificationUse: Regex =
    ("\\bOperational" + "KeyMaker\\s*\\.\\s*verify\\b").r

  private val rawKesPrimitiveUse: Regex =
    ("(?:\\b(?:Kes" + "Product|Kes" + "Sum)\\s*\\.\\s*instance\\b|\\bnew\\s+(?:KesProduct|KesSum)\\b)").r

  private val directAtomicRecordConstruction: Regex =
    ("\\bOperatorConsensus" + "Keys\\s*\\(").r

  private val splitRegistryFactoryCall: Regex = {
    val registryNames = Seq("Vrf", "Kes").map(_ + "Registry").mkString("(?:", "|", ")")
    s"\\b$registryNames\\s*\\.\\s*(?:make|empty)\\b".r
  }

  private val splitRegistryCompanion: Regex = {
    val registryNames = Seq("Vrf", "Kes").map(_ + "Registry").mkString("(?:", "|", ")")
    s"\\bobject\\s+$registryNames\\b".r
  }

  test("split KES or VRF registries cannot regain independent factories") {
    scalaSources.map { sources =>
      val factoryCalls = matchingPaths(sources, splitRegistryFactoryCall)
      val companions = matchingPaths(sources, splitRegistryCompanion)

      if (factoryCalls.isEmpty && companions.isEmpty) success
      else failure(s"split registry factory calls=${factoryCalls.mkString(",")} companions=${companions.mkString(",")}")
    }
  }

  test("production KES material can only be opened or generated at reviewed paired-key boundaries") {
    scalaSources.map { sources =>
      exactReviewedInventory(
        sources,
        "/src/main/scala/",
        rawKesProvisioningUse,
        reviewedKesProvisioningProduction,
        "production KES provisioning",
        excluded = Set(
          "modules/shared/src/main/scala/io/constellationnetwork/security/kes/OperationalKeyMaker.scala"
        )
      )
    }
  }

  test("test KES material provisioning remains canonical-fixture, primitive, or explicit-negative only") {
    scalaSources.map { sources =>
      exactReviewedInventory(
        sources,
        "/src/test/scala/",
        rawKesProvisioningUse,
        reviewedKesProvisioningTests,
        "test KES provisioning"
      )
    }
  }

  test("every raw production KES signing call is explicitly reviewed") {
    scalaSources.map { sources =>
      exactReviewedInventory(
        sources,
        "/src/main/scala/",
        rawKesSigningUse,
        reviewedKesSigningProduction,
        "production KES signing",
        excluded = Set(
          "modules/shared/src/main/scala/io/constellationnetwork/security/kes/OperationalKeyMaker.scala",
          "modules/shared/src/main/scala/io/constellationnetwork/security/kes/OperationalKeyMakerAlgebra.scala"
        )
      )
    }
  }

  test("every raw test KES signing call is explicitly reviewed") {
    scalaSources.map { sources =>
      exactReviewedInventory(
        sources,
        "/src/test/scala/",
        rawKesSigningUse,
        reviewedKesSigningTests,
        "test KES signing"
      )
    }
  }

  test("every raw production KES verification call is explicitly reviewed") {
    scalaSources.map { sources =>
      exactReviewedInventory(
        sources,
        "/src/main/scala/",
        rawKesVerificationUse,
        reviewedKesVerificationProduction,
        "production KES verification"
      )
    }
  }

  test("raw test KES verification remains inside reviewed canonical or isolated coverage") {
    scalaSources.map { sources =>
      exactReviewedInventory(
        sources,
        "/src/test/scala/",
        rawKesVerificationUse,
        reviewedKesVerificationTests,
        "test KES verification"
      )
    }
  }

  test("package-private KES primitives remain behind the lifecycle wrapper in production") {
    scalaSources.map { sources =>
      exactReviewedInventory(
        sources,
        "/src/main/scala/",
        rawKesPrimitiveUse,
        reviewedKesPrimitiveProduction,
        "production KES primitive use",
        excluded = Set(
          "modules/shared/src/main/scala/io/constellationnetwork/security/kes/KesProduct.scala",
          "modules/shared/src/main/scala/io/constellationnetwork/security/kes/KesSum.scala"
        )
      )
    }
  }

  test("direct KES primitive use remains confined to isolated primitive tests") {
    scalaSources.map { sources =>
      exactReviewedInventory(
        sources,
        "/src/test/scala/",
        rawKesPrimitiveUse,
        reviewedKesPrimitiveTests,
        "test KES primitive use"
      )
    }
  }

  test("every raw production VRF primitive consumer is explicitly reviewed") {
    scalaSources.map { sources =>
      val actualCounts = sources.iterator
        .filter(_.path.contains("/src/main/scala/"))
        .map(source => source.path -> rawVrfUse.findAllIn(withoutScalaComments(source.contents)).size)
        .filter { case (path, count) => count > 0 && !rawVrfPrimitiveImplementations.contains(path) }
        .toMap
      val actual = actualCounts.keySet
      val reviewed = reviewedRawVrfConsumers.keySet
      val unreviewed = actual -- reviewed
      val staleAllowances = reviewed -- actual
      val countInventoryMismatch =
        (reviewed -- reviewedRawVrfProductionCounts.keySet).union(reviewedRawVrfProductionCounts.keySet -- reviewed)
      val changedCounts = actual.intersect(reviewed).toList.sorted.collect {
        case path if reviewedRawVrfProductionCounts.get(path).exists(_ != actualCounts(path)) =>
          s"$path expected=${reviewedRawVrfProductionCounts(path)} actual=${actualCounts(path)}"
      }

      if (unreviewed.isEmpty && staleAllowances.isEmpty && countInventoryMismatch.isEmpty && changedCounts.isEmpty) success
      else
        failure(
          s"unreviewed raw VRF consumers=${unreviewed.toList.sorted.mkString(",")} " +
            s"stale reviewed entries=${staleAllowances.toList.sorted.mkString(",")} " +
            s"count/rationale key mismatch=${countInventoryMismatch.toList.sorted.mkString(",")} " +
            s"changed counts=${changedCounts.mkString(",")}"
        )
    }
  }

  test("every raw test VRF primitive consumer has an exact categorized review") {
    scalaSources.map { sources =>
      val actualCounts = sources.iterator
        .filter(_.path.contains("/src/test/scala/"))
        .map(source => source.path -> rawVrfUse.findAllIn(withoutScalaComments(source.contents)).size)
        .filter { case (_, count) => count > 0 }
        .toMap
      val actual = actualCounts.keySet
      val reviewedEntries = reviewedRawVrfTestConsumerCategories.flatMap {
        case (category, entries) => entries.keysIterator.map(path => path -> category)
      }
      val duplicateAllowances = reviewedEntries
        .groupBy(_._1)
        .collect { case (path, entries) if entries.sizeCompare(1) > 0 => s"$path:${entries.map(_._2).sorted.mkString("|")}" }
        .toList
        .sorted
      val reviewed = reviewedEntries.iterator.map(_._1).toSet
      val unreviewed = actual -- reviewed
      val staleAllowances = reviewed -- actual
      val countInventoryMismatch =
        (reviewed -- reviewedRawVrfTestCounts.keySet).union(reviewedRawVrfTestCounts.keySet -- reviewed)
      val changedCounts = actual.intersect(reviewed).toList.sorted.collect {
        case path if reviewedRawVrfTestCounts.get(path).exists(_ != actualCounts(path)) =>
          s"$path expected=${reviewedRawVrfTestCounts(path)} actual=${actualCounts(path)}"
      }

      if (
        unreviewed.isEmpty && staleAllowances.isEmpty && duplicateAllowances.isEmpty && countInventoryMismatch.isEmpty && changedCounts.isEmpty
      ) success
      else
        failure(
          s"unreviewed raw test VRF consumers=${unreviewed.toList.sorted.mkString(",")} " +
            s"stale reviewed entries=${staleAllowances.toList.sorted.mkString(",")} " +
            s"duplicate categorized entries=${duplicateAllowances.mkString(",")} " +
            s"count/rationale key mismatch=${countInventoryMismatch.toList.sorted.mkString(",")} " +
            s"changed counts=${changedCounts.mkString(",")}"
        )
    }
  }

  test("direct operator-pair construction remains confined to registry algebra and negative tests") {
    scalaSources.map { sources =>
      val actualCounts = sources.iterator
        .filter(_.path.contains("/src/test/scala/"))
        .map(source => source.path -> directAtomicRecordConstruction.findAllIn(withoutScalaComments(source.contents)).size)
        .filter { case (_, count) => count > 0 }
        .toMap
      val expectedCounts = reviewedDirectRecordFixtures.view.mapValues(_._1).toMap
      val unexpected = actualCounts.keySet -- expectedCounts.keySet
      val staleAllowances = expectedCounts.keySet -- actualCounts.keySet
      val changedCounts = actualCounts.keySet.intersect(expectedCounts.keySet).toList.sorted.collect {
        case path if actualCounts(path) != expectedCounts(path) =>
          s"$path expected=${expectedCounts(path)} actual=${actualCounts(path)}"
      }

      if (unexpected.isEmpty && staleAllowances.isEmpty && changedCounts.isEmpty) success
      else
        failure(
          s"unexpected constructors=${unexpected.toList.sorted.mkString(",")} " +
            s"stale reviewed entries=${staleAllowances.toList.sorted.mkString(",")} " +
            s"changed counts=${changedCounts.mkString(",")}"
        )
    }
  }

  test("production operator-pair construction remains confined to reviewed provenance boundaries") {
    scalaSources.map { sources =>
      val actualCounts = sources.iterator
        .filter(_.path.contains("/src/main/scala/"))
        .map(source => source.path -> directAtomicRecordConstruction.findAllIn(withoutScalaComments(source.contents)).size)
        .filter { case (_, count) => count > 0 }
        .toMap
      val expectedCounts = reviewedProductionDirectRecordSites.view.mapValues(_._1).toMap
      val unexpected = actualCounts.keySet -- expectedCounts.keySet
      val staleAllowances = expectedCounts.keySet -- actualCounts.keySet
      val changedCounts = actualCounts.keySet.intersect(expectedCounts.keySet).toList.sorted.collect {
        case path if actualCounts(path) != expectedCounts(path) =>
          s"$path expected=${expectedCounts(path)} actual=${actualCounts(path)}"
      }

      if (unexpected.isEmpty && staleAllowances.isEmpty && changedCounts.isEmpty) success
      else
        failure(
          s"unexpected production constructors=${unexpected.toList.sorted.mkString(",")} " +
            s"stale reviewed entries=${staleAllowances.toList.sorted.mkString(",")} " +
            s"changed counts=${changedCounts.mkString(",")}"
        )
    }
  }

  private def exactReviewedInventory(
    sources: List[Source],
    sourceSegment: String,
    pattern: Regex,
    reviewed: Map[String, (Int, String)],
    label: String,
    excluded: Set[String] = Set.empty
  ): weaver.Expectations = {
    val actualCounts = sources.iterator
      .filter(_.path.contains(sourceSegment))
      .filterNot(source => excluded.contains(source.path))
      .map(source => source.path -> pattern.findAllIn(withoutScalaComments(source.contents)).size)
      .filter { case (_, count) => count > 0 }
      .toMap
    val expectedCounts = reviewed.view.mapValues(_._1).toMap
    val unexpected = actualCounts.keySet -- expectedCounts.keySet
    val staleAllowances = expectedCounts.keySet -- actualCounts.keySet
    val changedCounts = actualCounts.keySet.intersect(expectedCounts.keySet).toList.sorted.collect {
      case path if actualCounts(path) != expectedCounts(path) =>
        s"$path expected=${expectedCounts(path)} actual=${actualCounts(path)}"
    }
    val emptyRationales = reviewed.iterator.collect { case (path, (_, rationale)) if rationale.trim.isEmpty => path }.toList.sorted

    if (unexpected.isEmpty && staleAllowances.isEmpty && changedCounts.isEmpty && emptyRationales.isEmpty) success
    else
      failure(
        s"$label unexpected=${unexpected.toList.sorted.mkString(",")} " +
          s"stale=${staleAllowances.toList.sorted.mkString(",")} " +
          s"changed counts=${changedCounts.mkString(",")} " +
          s"empty rationales=${emptyRationales.mkString(",")}"
      )
  }

  private def matchingPaths(sources: List[Source], pattern: Regex): List[String] =
    sources.iterator.filter(source => pattern.findFirstIn(withoutScalaComments(source.contents)).nonEmpty).map(_.path).toList.sorted

  /** Removes line and nested block comments without confusing comment markers inside string or character literals. Literal contents remain
    * searchable, intentionally: a source exception that names a raw primitive still requires review even when it is assembled reflectively.
    */
  private def withoutScalaComments(input: String): String = {
    val out = new StringBuilder(input.length)
    var index = 0
    var blockDepth = 0
    var inLineComment = false
    var inString = false
    var inTripleString = false
    var inCharacter = false
    var escaped = false

    while (index < input.length) {
      val current = input.charAt(index)
      val next = if (index + 1 < input.length) input.charAt(index + 1) else '\u0000'

      if (inLineComment) {
        if (current == '\n') {
          inLineComment = false
          out.append(current)
        }
        index += 1
      } else if (blockDepth > 0) {
        if (current == '/' && next == '*') {
          blockDepth += 1
          index += 2
        } else if (current == '*' && next == '/') {
          blockDepth -= 1
          index += 2
        } else {
          if (current == '\n') out.append(current)
          index += 1
        }
      } else if (inTripleString) {
        if (current == '"' && input.startsWith("\"\"\"", index)) {
          out.append("\"\"\"")
          index += 3
          inTripleString = false
        } else {
          out.append(current)
          index += 1
        }
      } else if (inString) {
        out.append(current)
        index += 1
        if (escaped) escaped = false
        else if (current == '\\') escaped = true
        else if (current == '"') inString = false
      } else if (inCharacter) {
        out.append(current)
        index += 1
        if (escaped) escaped = false
        else if (current == '\\') escaped = true
        else if (current == '\'') inCharacter = false
      } else if (current == '/' && next == '/') {
        inLineComment = true
        index += 2
      } else if (current == '/' && next == '*') {
        blockDepth = 1
        index += 2
      } else if (current == '"' && input.startsWith("\"\"\"", index)) {
        out.append("\"\"\"")
        index += 3
        inTripleString = true
      } else {
        out.append(current)
        index += 1
        if (current == '"') inString = true
        else if (current == '\'') inCharacter = true
      }
    }

    out.result()
  }

  private def scalaSources: IO[List[Source]] =
    IO.blocking {
      val root = repositoryRoot(Paths.get(sys.props("user.dir")).toAbsolutePath.normalize())
      val stream = Files.walk(root.resolve("modules"))
      try
        stream
          .iterator()
          .asScala
          .filter(Files.isRegularFile(_))
          .filter(_.getFileName.toString.endsWith(".scala"))
          .filter { path =>
            val normalized = path.toString.replace('\\', '/')
            normalized.contains("/src/main/scala/") || normalized.contains("/src/test/scala/")
          }
          .map { path =>
            val relative = root.relativize(path).toString.replace('\\', '/')
            Source(relative, new String(Files.readAllBytes(path), StandardCharsets.UTF_8))
          }
          .toList
      finally stream.close()
    }

  @annotation.tailrec
  private def repositoryRoot(candidate: Path): Path =
    if (Files.isDirectory(candidate.resolve("modules/node-shared")) && Files.isRegularFile(candidate.resolve("build.sbt"))) candidate
    else
      Option(candidate.getParent) match {
        case Some(parent) => repositoryRoot(parent)
        case None         => throw new IllegalStateException(s"Unable to locate repository root from ${sys.props("user.dir")}")
      }
}
