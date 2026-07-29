package io.constellationnetwork.serde.consensus

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import scala.jdk.CollectionConverters._

import io.constellationnetwork.schema.consensus.ArtifactAuthority.SourceAuthorization
import io.constellationnetwork.schema.consensus.ArtifactBinding._
import io.constellationnetwork.schema.consensus.ArtifactGap._
import io.constellationnetwork.schema.consensus.ConsensusArtifactKind._
import io.constellationnetwork.schema.consensus._
import io.constellationnetwork.serde.consensus.ConsensusArtifactRequirementsManifest.Violation._
import io.constellationnetwork.serde.consensus.ConsensusArtifactRequirementsManifest._

import weaver.FunSuite

object ConsensusArtifactRequirementsManifestSuite extends FunSuite {

  test("the declaration-only requirements inventories are structurally valid") {
    val artifactLabels = ConsensusArtifactKind.all.map(_.semanticLabel)
    val transcriptLabels = ConsensusTranscriptKind.all.map(_.semanticLabel)
    val finalityPayloadLabels = ConsensusFinalityPayloadKind.all.map(_.semanticLabel)
    val carrierLabels = ConsensusCarrierKind.all.map(_.semanticLabel)
    val chainSyncWireLabels = ConsensusChainSyncWireKind.all.map(_.semanticLabel)
    val allLabels =
      artifactLabels ++ transcriptLabels ++ finalityPayloadLabels ++ carrierLabels ++ chainSyncWireLabels ++
        ArtifactBinding.all.map(_.semanticLabel) ++ ArtifactAuthority.all.map(_.semanticLabel)
    val forbiddenBftWords = Set("vote", "voted", "lock", "locked", "qc")
    val forbiddenBftPhrases = Set("quorum-certificate", "view-change")
    val containsGlobalBftVocabulary = allLabels.exists { label =>
      val labelWords = label.split("[.-]").toSet
      val permittedEconomicWords = Option.when(label.contains("token-lock"))(Set("lock")).getOrElse(Set.empty)

      (labelWords -- permittedEconomicWords).exists(forbiddenBftWords.contains) || forbiddenBftPhrases.exists(label.contains)
    }

    expect.all(
      validation.isEmpty,
      artifactSchemaValidation.isEmpty,
      transcriptValidation.isEmpty,
      finalityPayloadValidation.isEmpty,
      carrierValidation.isEmpty,
      entries.size == 48,
      entries.map(_.kind).toSet == ConsensusArtifactKind.all.toSet,
      artifactSchemaDeclarations.size == 48,
      artifactSchemaDeclarations.map(_.kind).toSet == ConsensusArtifactKind.all.toSet,
      artifactSchemaDeclarations.map(_.kind.semanticLabel).distinct.size == artifactSchemaDeclarations.size,
      artifactSchemaDeclarations.forall(_.schemaStatus == ManifestSchemaStatus.SchemaOpen),
      artifactLabels.distinct.size == artifactLabels.size,
      ConsensusTranscriptKind.all.size == 10,
      transcriptEntries.size == 10,
      transcriptEntries.map(_.kind).toSet == ConsensusTranscriptKind.all.toSet,
      transcriptLabels.distinct.size == transcriptLabels.size,
      transcriptEntries.forall(_.authority == ArtifactAuthority.Eligibility),
      transcriptEntries.forall(_.bindings.contains(ExactEligibilityParent)),
      transcriptEntries.forall(_.bindings.contains(N2RosterStakeAndKeyView)),
      ConsensusFinalityPayloadKind.all.size == 14,
      finalityPayloadEntries.size == 14,
      finalityPayloadEntries.map(_.kind).toSet == ConsensusFinalityPayloadKind.all.toSet,
      finalityPayloadLabels.distinct.size == finalityPayloadLabels.size,
      ConsensusCarrierKind.all.size == 29,
      carrierEntries.size == 29,
      carrierEntries.map(_.kind).toSet == ConsensusCarrierKind.all.toSet,
      carrierLabels.distinct.size == carrierLabels.size,
      carrierEntries.forall(_.authority == ArtifactAuthority.TransportCoordinationOnly),
      ConsensusChainSyncWireKind.all.size == 12,
      chainSyncWireLabels.distinct.size == chainSyncWireLabels.size,
      ConsensusChainSyncWireKind.all.map(_.parentCarrier).toSet == Set(
        ConsensusCarrierKind.ChainSyncSnapshotCarrier,
        ConsensusCarrierKind.ChainSyncMetagraphBinaryCarrier,
        ConsensusCarrierKind.ChainSyncSelectionHint
      ),
      !containsGlobalBftVocabulary,
      codecStatus == ManifestCodecStatus.Open,
      activationStatus == ManifestActivationStatus.DarkOnly
    )
  }

  test("per-artifact schema declarations reject missing and duplicate rows while every schema remains open") {
    val duplicate = artifactSchemaDeclarations.head :: artifactSchemaDeclarations
    val missing = artifactSchemaDeclarations.filterNot(_.kind == MigrationManifest)

    expect.all(
      artifactSchemaDeclarations.forall(_.schemaStatus == ManifestSchemaStatus.SchemaOpen),
      validateArtifactSchemaDeclarations(duplicate).exists(_.isInstanceOf[DuplicateArtifactSchemaKind]),
      validateArtifactSchemaDeclarations(duplicate).exists(_.isInstanceOf[DuplicateArtifactSchemaSemanticLabel]),
      validateArtifactSchemaDeclarations(missing).exists(_.isInstanceOf[MissingArtifactSchemaKind])
    )
  }

  test("transport carriers bind their exact target without inheriting nested authority") {
    val nestedArtifact = Set[ArtifactBinding](ExactNestedArtifactIdentity, ExactNestedArtifactType)
    val sequencedNestedArtifact = nestedArtifact + OriginAndSequence
    val requestResponse = Set[ArtifactBinding](ExactRequestResponseCorrelation)
    val expectedSpecificBindings: Map[ConsensusCarrierKind, Set[ArtifactBinding]] = Map(
      ConsensusCarrierKind.PeerRumorEnvelope -> sequencedNestedArtifact,
      ConsensusCarrierKind.CommonRumorEnvelope -> nestedArtifact,
      ConsensusCarrierKind.SidecarRumorEnvelope -> nestedArtifact,
      ConsensusCarrierKind.EventGossipEnvelope -> nestedArtifact,
      ConsensusCarrierKind.EventGossipIHave -> requestResponse,
      ConsensusCarrierKind.EventGossipIWantRequest -> requestResponse,
      ConsensusCarrierKind.EventGossipIWantResponse -> (requestResponse ++ nestedArtifact),
      ConsensusCarrierKind.Ml0EventAnnouncementEnvelope -> sequencedNestedArtifact,
      ConsensusCarrierKind.Ml0FacilityEnvelope -> sequencedNestedArtifact,
      ConsensusCarrierKind.Ml0ProposalEnvelope -> sequencedNestedArtifact,
      ConsensusCarrierKind.Ml0MajoritySignatureEnvelope -> sequencedNestedArtifact,
      ConsensusCarrierKind.Ml0BinarySignatureEnvelope -> sequencedNestedArtifact,
      ConsensusCarrierKind.Ml0AckEnvelope -> sequencedNestedArtifact,
      ConsensusCarrierKind.Ml0WithdrawEnvelope -> sequencedNestedArtifact,
      ConsensusCarrierKind.Ml0ArtifactAnnouncementEnvelope -> nestedArtifact,
      ConsensusCarrierKind.PeerRumorInquiryRequest -> (requestResponse + OriginAndSequence),
      ConsensusCarrierKind.CommonRumorOfferResponse -> requestResponse,
      ConsensusCarrierKind.QueryCommonRumorsRequest -> requestResponse,
      ConsensusCarrierKind.CommonRumorInitResponse -> requestResponse,
      ConsensusCarrierKind.PeerRumorResponseStream -> (requestResponse ++ sequencedNestedArtifact),
      ConsensusCarrierKind.CommonRumorResponseStream -> (requestResponse ++ nestedArtifact),
      ConsensusCarrierKind.SidecarSubscribeRequest -> (requestResponse + SubscriptionProfile),
      ConsensusCarrierKind.SidecarSubscribeStarted ->
        (requestResponse ++ Set(SubscriptionProfile, SubscriptionSessionAndGeneration)),
      ConsensusCarrierKind.ChainSyncSnapshotCarrier -> (requestResponse ++ nestedArtifact),
      ConsensusCarrierKind.ChainSyncMetagraphBinaryCarrier -> (requestResponse ++ nestedArtifact),
      ConsensusCarrierKind.ChainSyncSelectionHint -> requestResponse,
      ConsensusCarrierKind.BootstrapMetadataHint -> (requestResponse ++ nestedArtifact),
      ConsensusCarrierKind.BootstrapPhase2StateBundle -> (requestResponse ++ nestedArtifact ++ Set(
        ExactPhase2Checkpoint,
        ExactPhase2QualificationEvidence,
        ComponentCompleteness,
        ChainSelectionWitness
      )),
      ConsensusCarrierKind.BootstrapGenesisBundle -> (requestResponse ++ nestedArtifact ++ Set(
        GenesisDeclaration,
        ComponentCompleteness
      ))
    )
    val actualBindings = carrierEntries.map(contract => contract.kind -> contract.bindings).toMap
    val expectedBindings = expectedSpecificBindings.view.mapValues(ArtifactBinding.carrierCommon ++ _).toMap
    val forbiddenAuthorities = ArtifactAuthority.all.toSet - ArtifactAuthority.TransportCoordinationOnly

    expect.all(
      actualBindings == expectedBindings,
      carrierEntries.forall(_.knownGaps.contains(O18TransportByteContractOpen)),
      carrierEntries.forall(contract => !forbiddenAuthorities.contains(contract.authority)),
      carrierEntries.forall(_.authority != ArtifactAuthority.StateValidity),
      carrierEntries.forall(_.authority != ArtifactAuthority.FinalityQualification),
      carrierEntries.forall(_.authority != ArtifactAuthority.ObjectiveEvidence),
      carrierEntries.forall(_.authority != ArtifactAuthority.SourceAuthorization)
    )
  }

  test("carrier validation rejects authority upgrades and every target-binding mutation") {
    val duplicate = carrierEntries.head :: carrierEntries
    val missing = carrierEntries.filterNot(_.kind == ConsensusCarrierKind.BootstrapGenesisBundle)
    val withoutCommon = carrierEntries.map {
      case contract if contract.kind == ConsensusCarrierKind.PeerRumorEnvelope =>
        contract.copy(bindings = contract.bindings - CarrierKind)
      case contract => contract
    }
    val withoutOrigin = carrierEntries.map {
      case contract if contract.kind == ConsensusCarrierKind.Ml0EventAnnouncementEnvelope =>
        contract.copy(bindings = contract.bindings - OriginAndSequence)
      case contract => contract
    }
    val withoutNestedType = carrierEntries.map {
      case contract if contract.kind == ConsensusCarrierKind.EventGossipEnvelope =>
        contract.copy(bindings = contract.bindings - ExactNestedArtifactType)
      case contract => contract
    }
    val withoutCorrelation = carrierEntries.map {
      case contract if contract.kind == ConsensusCarrierKind.ChainSyncSnapshotCarrier =>
        contract.copy(bindings = contract.bindings - ExactRequestResponseCorrelation)
      case contract => contract
    }
    val withoutPhase2Evidence = carrierEntries.map {
      case contract if contract.kind == ConsensusCarrierKind.BootstrapPhase2StateBundle =>
        contract.copy(bindings = contract.bindings - ExactPhase2QualificationEvidence)
      case contract => contract
    }
    val withoutPhase2Checkpoint = carrierEntries.map {
      case contract if contract.kind == ConsensusCarrierKind.BootstrapPhase2StateBundle =>
        contract.copy(bindings = contract.bindings - ExactPhase2Checkpoint)
      case contract => contract
    }
    val withoutCurrentChainWitness = carrierEntries.map {
      case contract if contract.kind == ConsensusCarrierKind.BootstrapPhase2StateBundle =>
        contract.copy(bindings = contract.bindings - ChainSelectionWitness)
      case contract => contract
    }
    val withoutCompleteness = carrierEntries.map {
      case contract if contract.kind == ConsensusCarrierKind.BootstrapGenesisBundle =>
        contract.copy(bindings = contract.bindings - ComponentCompleteness)
      case contract => contract
    }
    val withoutO18 = carrierEntries.map {
      case contract if contract.kind == ConsensusCarrierKind.EventGossipEnvelope =>
        contract.copy(knownGaps = contract.knownGaps - O18TransportByteContractOpen)
      case contract => contract
    }
    val withoutCanonicalRumorBytesGap = carrierEntries.map {
      case contract if contract.kind == ConsensusCarrierKind.SidecarRumorEnvelope =>
        contract.copy(knownGaps = contract.knownGaps - SignedRumorTransportBytesNotCanonical)
      case contract => contract
    }
    val withoutAggregateGap = carrierEntries.map {
      case contract if contract.kind == ConsensusCarrierKind.ChainSyncMetagraphBinaryCarrier =>
        contract.copy(knownGaps = contract.knownGaps - UnboundedAggregateTransportResponse)
      case contract => contract
    }
    val withoutLiveRumorAuthGap = carrierEntries.map {
      case contract if contract.kind == ConsensusCarrierKind.PeerRumorEnvelope =>
        contract.copy(knownGaps = contract.knownGaps - LiveRumorSourceAuthenticationUndecomposed)
      case contract => contract
    }
    val withoutSubscriptionProfile = carrierEntries.map {
      case contract if contract.kind == ConsensusCarrierKind.SidecarSubscribeStarted =>
        contract.copy(bindings = contract.bindings - SubscriptionSessionAndGeneration)
      case contract => contract
    }
    val wrongStatus = carrierEntries.map {
      case contract if contract.kind == ConsensusCarrierKind.BootstrapGenesisBundle =>
        contract.copy(definitionStatus = ArtifactDefinitionStatus.ExistingShapeNeedsAudit)
      case contract => contract
    }
    val authorityUpgrades = (ArtifactAuthority.all.toSet - ArtifactAuthority.TransportCoordinationOnly).toList.map { authority =>
      carrierEntries.map {
        case contract if contract.kind == ConsensusCarrierKind.PeerRumorEnvelope => contract.copy(authority = authority)
        case contract                                                            => contract
      }
    }
    val allBindingRemovalMutants = carrierEntries.flatMap { target =>
      target.bindings.toList.map { removed =>
        val mutant = carrierEntries.map {
          case contract if contract.kind == target.kind => contract.copy(bindings = contract.bindings - removed)
          case contract                                 => contract
        }
        (target.kind, removed, mutant)
      }
    }

    expect.all(
      validateCarrierEntries(duplicate).exists(_.isInstanceOf[DuplicateCarrierKind]),
      validateCarrierEntries(duplicate).exists(_.isInstanceOf[DuplicateCarrierSemanticLabel]),
      validateCarrierEntries(missing).exists(_.isInstanceOf[MissingCarrierKind]),
      validateCarrierEntries(withoutCommon).exists(_.isInstanceOf[MissingCarrierCommonBindings]),
      validateCarrierEntries(withoutOrigin).exists(_.isInstanceOf[MissingCarrierBindings]),
      validateCarrierEntries(withoutNestedType).exists(_.isInstanceOf[MissingCarrierBindings]),
      validateCarrierEntries(withoutCorrelation).exists(_.isInstanceOf[MissingCarrierBindings]),
      validateCarrierEntries(withoutPhase2Evidence).exists(_.isInstanceOf[MissingCarrierBindings]),
      validateCarrierEntries(withoutPhase2Checkpoint).exists(_.isInstanceOf[MissingCarrierBindings]),
      validateCarrierEntries(withoutCurrentChainWitness).exists(_.isInstanceOf[MissingCarrierBindings]),
      validateCarrierEntries(withoutCompleteness).exists(_.isInstanceOf[MissingCarrierBindings]),
      validateCarrierEntries(withoutO18).exists(_.isInstanceOf[MissingCarrierGap]),
      validateCarrierEntries(withoutCanonicalRumorBytesGap).exists(_.isInstanceOf[MissingCarrierGap]),
      validateCarrierEntries(withoutAggregateGap).exists(_.isInstanceOf[MissingCarrierGap]),
      validateCarrierEntries(withoutLiveRumorAuthGap).exists(_.isInstanceOf[MissingCarrierGap]),
      validateCarrierEntries(withoutSubscriptionProfile).exists(_.isInstanceOf[MissingCarrierBindings]),
      validateCarrierEntries(wrongStatus).exists(_.isInstanceOf[InvalidCarrierDefinitionStatus]),
      authorityUpgrades.forall(contracts => validateCarrierEntries(contracts).exists(_.isInstanceOf[InvalidCarrierAuthority])),
      allBindingRemovalMutants.forall {
        case (kind, removed, contracts) =>
          validateCarrierEntries(contracts).exists {
            case MissingCarrierCommonBindings(observed, missing) => observed == kind && missing.contains(removed)
            case MissingCarrierBindings(observed, missing)       => observed == kind && missing.contains(removed)
            case _                                               => false
          }
      }
    )
  }

  test("carrier rows preserve the audited current transport gaps") {
    val byKind = carrierEntries.map(contract => contract.kind -> contract).toMap
    val rumorGaps = Set[ArtifactGap](
      DelimiterFreeRumorSignaturePreimage,
      RuntimeScalaTypeStringDiscriminator,
      LiveRumorSourceAuthenticationUndecomposed,
      O18TransportByteContractOpen
    )
    val ml0RumorKinds = Set[ConsensusCarrierKind](
      ConsensusCarrierKind.Ml0EventAnnouncementEnvelope,
      ConsensusCarrierKind.Ml0FacilityEnvelope,
      ConsensusCarrierKind.Ml0ProposalEnvelope,
      ConsensusCarrierKind.Ml0MajoritySignatureEnvelope,
      ConsensusCarrierKind.Ml0BinarySignatureEnvelope,
      ConsensusCarrierKind.Ml0AckEnvelope,
      ConsensusCarrierKind.Ml0WithdrawEnvelope,
      ConsensusCarrierKind.Ml0ArtifactAnnouncementEnvelope
    )

    expect.all(
      byKind(ConsensusCarrierKind.PeerRumorEnvelope).knownGaps == rumorGaps,
      byKind(ConsensusCarrierKind.CommonRumorEnvelope).knownGaps == rumorGaps,
      byKind(ConsensusCarrierKind.SidecarRumorEnvelope).knownGaps == Set(
        TransportHintLacksPortableArtifactBinding,
        SignedRumorTransportBytesNotCanonical,
        LossyPeerRumorGapRepairMissing,
        O18TransportByteContractOpen
      ),
      ml0RumorKinds.forall(kind => byKind(kind).knownGaps == rumorGaps + NestedMl0ConsensusArtifactManifestOpen),
      byKind(ConsensusCarrierKind.EventGossipEnvelope).knownGaps == Set(
        NestedArtifactIdentityNotEnforced,
        O18TransportByteContractOpen
      ),
      byKind(ConsensusCarrierKind.EventGossipIHave).knownGaps == Set(
        TransportHintLacksPortableArtifactBinding,
        MissingTransportRequestIdentity,
        O18TransportByteContractOpen
      ),
      byKind(ConsensusCarrierKind.EventGossipIWantRequest).knownGaps == Set(
        MissingTransportRequestIdentity,
        O18TransportByteContractOpen
      ),
      byKind(ConsensusCarrierKind.EventGossipIWantResponse).knownGaps == Set(
        MissingTransportRequestIdentity,
        NestedArtifactIdentityNotEnforced,
        O18TransportByteContractOpen
      ),
      byKind(ConsensusCarrierKind.PeerRumorInquiryRequest).knownGaps == Set(
        MissingTransportRequestIdentity,
        O18TransportByteContractOpen
      ),
      byKind(ConsensusCarrierKind.CommonRumorOfferResponse).knownGaps == Set(
        TransportHintLacksPortableArtifactBinding,
        MissingTransportRequestIdentity,
        O18TransportByteContractOpen
      ),
      byKind(ConsensusCarrierKind.QueryCommonRumorsRequest).knownGaps == Set(
        MissingTransportRequestIdentity,
        O18TransportByteContractOpen
      ),
      byKind(ConsensusCarrierKind.CommonRumorInitResponse).knownGaps == Set(
        TransportHintLacksPortableArtifactBinding,
        MissingTransportRequestIdentity,
        O18TransportByteContractOpen
      ),
      byKind(ConsensusCarrierKind.PeerRumorResponseStream).knownGaps ==
        rumorGaps ++ Set(MissingTransportRequestIdentity, UnboundedAggregateTransportResponse),
      byKind(ConsensusCarrierKind.CommonRumorResponseStream).knownGaps ==
        rumorGaps ++ Set(MissingTransportRequestIdentity, UnboundedAggregateTransportResponse),
      byKind(ConsensusCarrierKind.SidecarSubscribeRequest).knownGaps == Set(
        LocalSubscriptionProductionGateControl,
        MissingTransportRequestIdentity,
        O18TransportByteContractOpen
      ),
      byKind(ConsensusCarrierKind.SidecarSubscribeStarted).knownGaps == Set(
        LocalSubscriptionProductionGateControl,
        MissingTransportRequestIdentity,
        O18TransportByteContractOpen
      ),
      byKind(ConsensusCarrierKind.ChainSyncSnapshotCarrier).knownGaps == Set(
        TransportDispositionLacksPortableArtifactBinding,
        MissingTransportRequestIdentity,
        NestedArtifactIdentityNotEnforced,
        UnboundedAggregateTransportResponse,
        O18TransportByteContractOpen
      ),
      byKind(ConsensusCarrierKind.ChainSyncMetagraphBinaryCarrier).knownGaps == Set(
        MissingTransportRequestIdentity,
        NestedArtifactIdentityNotEnforced,
        UnboundedAggregateTransportResponse,
        O18TransportByteContractOpen
      ),
      byKind(ConsensusCarrierKind.ChainSyncSelectionHint).knownGaps == Set(
        TransportHintLacksPortableArtifactBinding,
        MissingTransportRequestIdentity,
        O18TransportByteContractOpen
      ),
      byKind(ConsensusCarrierKind.BootstrapMetadataHint).knownGaps == Set(
        TransportHintLacksPortableArtifactBinding,
        MissingTransportRequestIdentity,
        O18TransportByteContractOpen
      ),
      byKind(ConsensusCarrierKind.BootstrapPhase2StateBundle).knownGaps == Set(
        MissingTransportRequestIdentity,
        BootstrapPhase2BundleSchemaOpen,
        BootstrapCurrentChainWitnessSchemaOpen,
        O18TransportByteContractOpen
      ),
      byKind(ConsensusCarrierKind.BootstrapGenesisBundle).knownGaps == Set(
        MissingTransportRequestIdentity,
        BootstrapGenesisBundleSchemaOpen,
        O18TransportByteContractOpen
      )
    )
  }

  test("sidecar rumor ID containment and ChainSync aggregate gaps remain source-grounded") {
    val root = repositoryRoot(Paths.get(sys.props("user.dir")).toAbsolutePath.normalize())
    def source(path: String): String =
      new String(Files.readAllBytes(root.resolve(path)), StandardCharsets.UTF_8)

    val sidecarProto = source("p2p/proto/sidecar.proto")
    val gossipGo = source("p2p/internal/gossip/gossip.go")
    val rumorBridge = source(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/consensus/nakamoto/SidecarRumorBridge.scala"
    )
    val chainSyncGo = source("p2p/internal/chainsync/protocol.go")
    val chainSyncManager = source(
      "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/ChainSyncManager.scala"
    )
    val carrierByKind = carrierEntries.map(contract => contract.kind -> contract).toMap

    expect.all(
      sidecarProto.contains("string content_type = 2;"),
      sidecarProto.contains("bytes origin_id = 3;"),
      gossipGo.contains("gossipMessageID(pmsg.GetTopic(), cfg.RumorTopic, pmsg.GetData())"),
      gossipGo.contains("func rumorEnvelopeSignedBytes(data []byte) ([]byte, bool)"),
      gossipGo.contains("tagLength < 0 || !number.IsValid() || tagLength != protowire.SizeTag(number)"),
      gossipGo.contains("number <= lastNumber || number > 3 || wireType != protowire.BytesType"),
      gossipGo.contains("lengthSize < 0 || lengthSize != protowire.SizeVarint(length)"),
      gossipGo.contains("if !utf8.Valid(value)"),
      gossipGo.contains("rumorInvalidMessageIDDomain"),
      !gossipGo.contains("return contentMessageID(pmsg.GetTopic(), pmsg.GetData())"),
      rumorBridge.contains("val bytes = rumor.signedRumorBytes.toByteArray"),
      carrierByKind(ConsensusCarrierKind.SidecarRumorEnvelope).knownGaps.contains(SignedRumorTransportBytesNotCanonical),
      chainSyncGo.contains("MaxMessageSize      = 16 * 1024 * 1024"),
      !chainSyncGo.contains("MaxAggregateFetchBytes"),
      chainSyncGo.contains("for {\n\t\tdata, err := readLengthPrefixed(s)"),
      chainSyncManager.contains("stub.fetchSnapshots(request).toList"),
      chainSyncManager.contains("stub.fetchMetagraphBinaries(request).toList"),
      carrierByKind(ConsensusCarrierKind.ChainSyncSnapshotCarrier).knownGaps.contains(UnboundedAggregateTransportResponse),
      carrierByKind(ConsensusCarrierKind.ChainSyncMetagraphBinaryCarrier).knownGaps.contains(UnboundedAggregateTransportResponse)
    )
  }

  test("live rumor authentication, response streams, and subscription production control remain explicit") {
    val root = repositoryRoot(Paths.get(sys.props("user.dir")).toAbsolutePath.normalize())
    def source(path: String): String =
      new String(Files.readAllBytes(root.resolve(path)), StandardCharsets.UTF_8)

    val daemon = source(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/gossip/GossipDaemon.scala"
    )
    val validator = source(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/gossip/RumorValidator.scala"
    )
    val gossipClient = source(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/gossip/p2p/GossipClient.scala"
    )
    val rumorBridge = source(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/consensus/nakamoto/SidecarRumorBridge.scala"
    )
    val gossipStream = source(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/consensus/nakamoto/GossipStream.scala"
    )
    val readiness = source(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/consensus/nakamoto/SidecarSubscriptionReadiness.scala"
    )
    val byKind = carrierEntries.map(contract => contract.kind -> contract).toMap

    expect.all(
      daemon.contains("validateRumor(hashedRumor)"),
      daemon.contains(".evalMap(handleRumor)"),
      validator.contains(".productR(validateOrigin(signedRumor))"),
      validator.contains(".productR(validateSeedlist(signedRumor))"),
      byKind(ConsensusCarrierKind.PeerRumorEnvelope).knownGaps.contains(LiveRumorSourceAuthenticationUndecomposed),
      byKind(ConsensusCarrierKind.CommonRumorEnvelope).knownGaps.contains(LiveRumorSourceAuthenticationUndecomposed),
      gossipClient.contains("PeerResponse[Stream[F, *], Signed[PeerRumorRaw]]"),
      gossipClient.contains("PeerResponse[Stream[F, *], Signed[CommonRumorRaw]]"),
      rumorBridge.contains("rumorQueue.tryOffer(hashed)"),
      daemon.contains("if (nakamotoMode) consumeRumors"),
      byKind(ConsensusCarrierKind.SidecarRumorEnvelope).knownGaps.contains(LossyPeerRumorGapRepairMissing),
      gossipStream.contains("validateStarted(profile, started) >> readiness.acknowledge"),
      readiness.contains("productionGate.pause(SnapshotProductionGate.InboundSubscriptionsUnavailable)"),
      readiness.contains("productionGate.resume(SnapshotProductionGate.InboundSubscriptionsUnavailable)"),
      byKind(ConsensusCarrierKind.SidecarSubscribeStarted).knownGaps.contains(LocalSubscriptionProductionGateControl)
    )
  }

  test("the ChainSync source-shape inventory is closed over the current protobuf services") {
    val root = repositoryRoot(Paths.get(sys.props("user.dir")).toAbsolutePath.normalize())
    val proto = new String(Files.readAllBytes(root.resolve("p2p/proto/sidecar.proto")), StandardCharsets.UTF_8)
    val expectedMessages = Set(
      "ChainPoint",
      "FetchSnapshotsRequest",
      "Snapshot",
      "FindIntersectionRequest",
      "FindIntersectionResponse",
      "GetPeerTipRequest",
      "PeerTipResponse",
      "ServeSnapshotsRequest",
      "ServeChainPointsRequest",
      "ServeChainPointsResponse",
      "FetchMetagraphBinariesRequest",
      "MetagraphBinaryResponse"
    )
    val chainSyncSection = proto.substring(proto.indexOf("message ChainPoint"))
    val declaredChainSyncMessages = "(?m)^message ([A-Za-z][A-Za-z0-9]*)".r
      .findAllMatchIn(chainSyncSection)
      .map(_.group(1))
      .toSet

    expect.all(
      ConsensusChainSyncWireKind.all.size == expectedMessages.size,
      declaredChainSyncMessages == expectedMessages - "Snapshot",
      proto.contains("message Snapshot"),
      proto.contains("rpc FetchSnapshots(FetchSnapshotsRequest) returns (stream Snapshot)"),
      proto.contains("rpc ServeMetagraphBinaries(FetchMetagraphBinariesRequest) returns (stream MetagraphBinaryResponse)")
    )
  }

  test("finality payload authority and concrete codec/vector evidence stay narrowly scoped") {
    val byKind = finalityPayloadEntries.map(contract => contract.kind -> contract).toMap
    val qualificationKinds = finalityPayloadEntries.collect {
      case contract if contract.authority == ArtifactAuthority.FinalityQualification => contract.kind
    }.toSet
    val concreteKinds = finalityPayloadEntries.collect {
      case contract if contract.codecStatus == FinalityPayloadCodecStatus.ConcreteCanonicalCodec => contract.kind
    }.toSet
    val vectorKinds = finalityPayloadEntries.collect {
      case contract if contract.vectorStatus == FinalityPayloadVectorStatus.ConcreteFrozenVector => contract.kind
    }.toSet
    val concreteSourceTypes = finalityPayloadEntries.collect {
      case contract @ FinalityPayloadContract(_, FinalityPayloadShape.ConcreteSourceType(sourceType), _, _, _, _, _, _) =>
        contract.kind -> sourceType
    }.toMap
    val expectedConcrete = Set[ConsensusFinalityPayloadKind](
      ConsensusFinalityPayloadKind.CoreBatch,
      ConsensusFinalityPayloadKind.ReleasedCoreRecord,
      ConsensusFinalityPayloadKind.PathManifest,
      ConsensusFinalityPayloadKind.PathChunk
    )
    val expectedSourceTypes = Map[ConsensusFinalityPayloadKind, String](
      ConsensusFinalityPayloadKind.CoreBatch ->
        "io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityCoreBatch",
      ConsensusFinalityPayloadKind.ReleasedCoreRecord ->
        "io.constellationnetwork.node.shared.domain.snapshot.finality.ReleasedCoreRecordPayload",
      ConsensusFinalityPayloadKind.PathManifest ->
        "io.constellationnetwork.node.shared.domain.snapshot.finality.PathManifestPayload",
      ConsensusFinalityPayloadKind.PathChunk ->
        "io.constellationnetwork.node.shared.domain.snapshot.finality.PathChunk"
    )

    expect.all(
      qualificationKinds == Set[ConsensusFinalityPayloadKind](
        ConsensusFinalityPayloadKind.DecidedAttestationEvidence,
        ConsensusFinalityPayloadKind.DepthK1Evidence
      ),
      byKind(ConsensusFinalityPayloadKind.ForkChoiceDecisionEvidence).authority == ArtifactAuthority.ObjectiveEvidence,
      byKind(ConsensusFinalityPayloadKind.PathManifest).authority == ArtifactAuthority.Commitment,
      byKind(ConsensusFinalityPayloadKind.PathChunk).authority == ArtifactAuthority.Commitment,
      finalityPayloadEntries
        .filterNot(contract => qualificationKinds.contains(contract.kind))
        .forall(_.authority != ArtifactAuthority.FinalityQualification),
      finalityPayloadEntries.forall(_.authority != ArtifactAuthority.StateValidity),
      concreteKinds == expectedConcrete,
      vectorKinds == expectedConcrete,
      concreteSourceTypes == expectedSourceTypes,
      finalityPayloadEntries.filter(contract => expectedConcrete.contains(contract.kind)).forall(_.knownGaps.isEmpty),
      finalityPayloadEntries
        .filterNot(contract => expectedConcrete.contains(contract.kind))
        .forall(contract =>
          contract.payloadShape == FinalityPayloadShape.OpaquePointerOnly &&
            contract.knownGaps.contains(FinalityOpaquePayloadSchemaMissing)
        )
    )
  }

  test("finality payload validation rejects missing, duplicate, underbound, and overstated rows") {
    val duplicate = finalityPayloadEntries.head :: finalityPayloadEntries
    val missing = finalityPayloadEntries.filterNot(_.kind == ConsensusFinalityPayloadKind.EffectPayload)
    val wrongAuthority = finalityPayloadEntries.map {
      case contract if contract.kind == ConsensusFinalityPayloadKind.ForkChoiceDecisionEvidence =>
        contract.copy(authority = ArtifactAuthority.FinalityQualification)
      case contract => contract
    }
    val withoutCommonBinding = finalityPayloadEntries.map {
      case contract if contract.kind == ConsensusFinalityPayloadKind.CoreBatch =>
        contract.copy(bindings = contract.bindings - ParameterHash)
      case contract => contract
    }
    val withoutQualificationContext = finalityPayloadEntries.map {
      case contract if contract.kind == ConsensusFinalityPayloadKind.DecidedAttestationEvidence =>
        contract.copy(bindings = contract.bindings - AttestationDecisionContext)
      case contract => contract
    }
    val opaqueClaimingConcrete = finalityPayloadEntries.map {
      case contract if contract.kind == ConsensusFinalityPayloadKind.EffectPayload =>
        contract.copy(
          payloadShape = FinalityPayloadShape.ConcreteSourceType("invented.EffectPayload"),
          codecStatus = FinalityPayloadCodecStatus.ConcreteCanonicalCodec,
          vectorStatus = FinalityPayloadVectorStatus.ConcreteFrozenVector
        )
      case contract => contract
    }
    val concreteClaimingOpaque = finalityPayloadEntries.map {
      case contract if contract.kind == ConsensusFinalityPayloadKind.CoreBatch =>
        contract.copy(
          payloadShape = FinalityPayloadShape.OpaquePointerOnly,
          codecStatus = FinalityPayloadCodecStatus.NoCanonicalPayloadCodec,
          vectorStatus = FinalityPayloadVectorStatus.NoCanonicalPayloadVector
        )
      case contract => contract
    }
    val wrongDefinitionStatus = finalityPayloadEntries.map {
      case contract if contract.kind == ConsensusFinalityPayloadKind.PathChunk =>
        contract.copy(definitionStatus = ArtifactDefinitionStatus.TargetShapeOpen)
      case contract => contract
    }
    val withoutOpaqueGap = finalityPayloadEntries.map {
      case contract if contract.kind == ConsensusFinalityPayloadKind.PriorAnchorReceipt =>
        contract.copy(knownGaps = contract.knownGaps - FinalityOpaquePayloadSchemaMissing)
      case contract => contract
    }

    expect.all(
      validateFinalityPayloadEntries(duplicate).exists(_.isInstanceOf[DuplicateFinalityPayloadKind]),
      validateFinalityPayloadEntries(duplicate).exists(_.isInstanceOf[DuplicateFinalityPayloadSemanticLabel]),
      validateFinalityPayloadEntries(missing).exists(_.isInstanceOf[MissingFinalityPayloadKind]),
      validateFinalityPayloadEntries(wrongAuthority).exists(_.isInstanceOf[InvalidFinalityPayloadAuthority]),
      validateFinalityPayloadEntries(withoutCommonBinding).exists(_.isInstanceOf[MissingFinalityPayloadCommonBindings]),
      validateFinalityPayloadEntries(withoutQualificationContext)
        .exists(_.isInstanceOf[MissingFinalityPayloadAuthorityBindings]),
      validateFinalityPayloadEntries(opaqueClaimingConcrete).exists(_.isInstanceOf[InvalidFinalityPayloadEvidence]),
      validateFinalityPayloadEntries(concreteClaimingOpaque).exists(_.isInstanceOf[InvalidFinalityPayloadEvidence]),
      validateFinalityPayloadEntries(wrongDefinitionStatus).exists(_.isInstanceOf[InvalidFinalityPayloadDefinitionStatus]),
      validateFinalityPayloadEntries(withoutOpaqueGap).exists(_.isInstanceOf[MissingFinalityPayloadGap])
    )
  }

  test("known consensus-byte gaps remain explicit and cannot be mistaken for codec readiness") {
    def gaps(kind: ConsensusArtifactKind): Set[ArtifactGap] =
      entries.find(_.kind == kind).fold(Set.empty[ArtifactGap])(_.knownGaps)

    expect.all(
      gaps(GlobalStateAccumulator) == Set(AccumulatorOmitsConsumedAllowSpends, AccumulatorOmitsSlashings),
      gaps(GlobalFollowDelta) == Set(
        AccumulatorOmitsConsumedAllowSpends,
        AccumulatorOmitsSlashings,
        ChangeSetCannotCarryCompleteGlobalDelta
      ),
      gaps(GlobalMptKey) == Set(GlobalMptPhysicalKeyGrammarUnenforced),
      gaps(GlobalMptValue) == Set(SlashingValueCodecUsesJson),
      gaps(OptimisticTipAttestation) == Set(
        OptimisticTipAttestationMissingExactContext,
        OptimisticTipAttestationCarriesWallClock
      ),
      gaps(TowerProof) == Set(PortableTowerHistoricalEligibilityMissing),
      gaps(GlobalGenesisSnapshot) == Set(OrdinalZeroGenesisUsesLegacyShape),
      gaps(CurrencyGenesisSnapshot) == Set(OrdinalZeroGenesisUsesLegacyShape),
      gaps(MigrationManifest) == Set(O19MigrationPolicyOpen)
    )
  }

  test("selection transcripts require exact branch-historical preregistration evidence") {
    val duplicate = transcriptEntries.head :: transcriptEntries
    val withoutN2View = transcriptEntries.map {
      case contract if contract.kind == ConsensusTranscriptKind.AdmissionVrf =>
        contract.copy(bindings = contract.bindings - N2RosterStakeAndKeyView)
      case contract => contract
    }
    val withoutAdmissionParent = transcriptEntries.map {
      case contract if contract.kind == ConsensusTranscriptKind.AdmissionVrf =>
        contract.copy(bindings = contract.bindings - ExactSourceParent)
      case contract => contract
    }
    val withoutEligibilityParent = transcriptEntries.map {
      case contract if contract.kind == ConsensusTranscriptKind.Gl0LeaderVrf =>
        contract.copy(bindings = contract.bindings - ExactEligibilityParent)
      case contract => contract
    }
    val wrongAuthority = transcriptEntries.map {
      case contract if contract.kind == ConsensusTranscriptKind.TowerEligibility =>
        contract.copy(authority = SourceAuthorization)
      case contract => contract
    }
    val withoutTower = transcriptEntries.filterNot(_.kind == ConsensusTranscriptKind.TowerEligibility)

    expect.all(
      validateTranscriptEntries(duplicate).exists(_.isInstanceOf[DuplicateTranscriptKind]),
      validateTranscriptEntries(duplicate).exists(_.isInstanceOf[DuplicateTranscriptSemanticLabel]),
      validateTranscriptEntries(withoutN2View).exists(_.isInstanceOf[MissingTranscriptBindings]),
      validateTranscriptEntries(withoutAdmissionParent).exists(_.isInstanceOf[MissingTranscriptBindings]),
      validateTranscriptEntries(withoutEligibilityParent).exists(_.isInstanceOf[MissingTranscriptBindings]),
      validateTranscriptEntries(wrongAuthority).exists(_.isInstanceOf[InvalidTranscriptAuthority]),
      validateTranscriptEntries(withoutTower).exists(_.isInstanceOf[MissingTranscriptKind])
    )
  }

  test("the pure validator rejects duplicate, incomplete, and authority-underbound declarations") {
    val duplicate = entries.head :: entries
    val withoutNetwork = entries.map {
      case contract if contract.kind == NativeDagBlock => contract.copy(bindings = contract.bindings - Network)
      case contract                                    => contract
    }
    val withoutDecisionContext = entries.map {
      case contract if contract.kind == DecidedOptimisticAttestationEvidence =>
        contract.copy(bindings = contract.bindings - AttestationDecisionContext)
      case contract => contract
    }
    val withoutWatchtowerCheckpoint = entries.map {
      case contract if contract.kind == PositiveWatchtowerCoverage =>
        contract.copy(bindings = contract.bindings - ExactCheckpoint)
      case contract => contract
    }
    val withoutWatchtowerAssignment = entries.map {
      case contract if contract.kind == PositiveWatchtowerCoverage =>
        contract.copy(bindings = contract.bindings - WatchtowerAssignmentContext)
      case contract => contract
    }
    val withoutMigration = entries.filterNot(_.kind == MigrationManifest)

    expect.all(
      validateEntries(duplicate).exists(_.isInstanceOf[DuplicateKind]),
      validateEntries(duplicate).exists(_.isInstanceOf[DuplicateSemanticLabel]),
      validateEntries(withoutNetwork).exists(_.isInstanceOf[MissingCommonBindings]),
      validateEntries(withoutDecisionContext).exists(_.isInstanceOf[MissingAuthorityBindings]),
      validateEntries(withoutWatchtowerCheckpoint).exists(_.isInstanceOf[MissingArtifactBindings]),
      validateEntries(withoutWatchtowerAssignment).exists(_.isInstanceOf[MissingArtifactBindings]),
      validateEntries(withoutMigration).exists(_.isInstanceOf[MissingKind])
    )
  }

  test("requirements metadata remains unreachable from production runtime code") {
    val root = repositoryRoot(Paths.get(sys.props("user.dir")).toAbsolutePath.normalize())
    val sources = productionSources(root)
    val reviewedMetadataPaths = Set(
      "modules/shared/src/main/scala/io/constellationnetwork/schema/consensus/ArtifactAuthority.scala",
      "modules/shared/src/main/scala/io/constellationnetwork/schema/consensus/ArtifactBinding.scala",
      "modules/shared/src/main/scala/io/constellationnetwork/schema/consensus/ConsensusCarrierKind.scala",
      "modules/shared/src/main/scala/io/constellationnetwork/schema/consensus/ConsensusChainSyncWireKind.scala",
      "modules/shared/src/main/scala/io/constellationnetwork/schema/consensus/ConsensusFinalityPayloadKind.scala",
      "modules/shared/src/main/scala/io/constellationnetwork/schema/consensus/ConsensusArtifactKind.scala",
      "modules/shared/src/main/scala/io/constellationnetwork/schema/consensus/ConsensusArtifactStatus.scala",
      "modules/shared/src/main/scala/io/constellationnetwork/schema/consensus/ConsensusTranscriptKind.scala",
      "modules/shared/src/main/scala/io/constellationnetwork/serde/consensus/ConsensusArtifactRequirementsManifest.scala"
    )
    val metadataSymbols = List(
      "ArtifactAuthority",
      "ArtifactBinding",
      "ArtifactDefinitionStatus",
      "ArtifactGap",
      "ManifestCodecStatus",
      "ManifestSchemaStatus",
      "ManifestActivationStatus",
      "ConsensusArtifactKind",
      "ConsensusCarrierKind",
      "ConsensusChainSyncWireKind",
      "ConsensusFinalityPayloadKind",
      "FinalityPayloadShape",
      "FinalityPayloadCodecStatus",
      "FinalityPayloadVectorStatus",
      "ConsensusTranscriptKind",
      "ArtifactSchemaDeclaration",
      "TranscriptContract",
      "CarrierContract",
      "FinalityPayloadContract",
      "ConsensusArtifactRequirementsManifest"
    )
    val metadataReferences = sources.collect {
      case (path, contents) if metadataSymbols.exists(contents.contains) => path
    }.toSet
    val metadataContents = reviewedMetadataPaths.toList.map { path =>
      new String(Files.readAllBytes(root.resolve(path)), StandardCharsets.UTF_8)
    }
    val forbiddenRuntimeTypes = List("Hasher", "ImmutableCodec", "ProtocolEraId", "scodec.")

    expect.all(
      metadataReferences == reviewedMetadataPaths,
      metadataContents.forall(contents => forbiddenRuntimeTypes.forall(token => !contents.contains(token)))
    )
  }

  private def productionSources(root: Path): List[(String, String)] = {
    val stream = Files.walk(root.resolve("modules"))
    try
      stream
        .iterator()
        .asScala
        .filter(Files.isRegularFile(_))
        .filter(_.getFileName.toString.endsWith(".scala"))
        .filter(_.toString.replace('\\', '/').contains("/src/main/scala/"))
        .map { path =>
          root.relativize(path).toString.replace('\\', '/') -> new String(Files.readAllBytes(path), StandardCharsets.UTF_8)
        }
        .toList
    finally stream.close()
  }

  @annotation.tailrec
  private def repositoryRoot(candidate: Path): Path =
    if (Files.isDirectory(candidate.resolve("modules/shared")) && Files.isRegularFile(candidate.resolve("build.sbt"))) candidate
    else
      Option(candidate.getParent) match {
        case Some(parent) => repositoryRoot(parent)
        case None         => throw new IllegalStateException(s"Unable to locate repository root from ${sys.props("user.dir")}")
      }
}
