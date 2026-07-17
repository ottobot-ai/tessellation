package io.constellationnetwork.node.shared.domain.nakamoto

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import cats.effect.IO
import cats.syntax.all._

import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.util.matching.Regex

import weaver.SimpleIOSuite

/** Machine-checks the reviewed higher-level KES/VRF consumer manifest.
  *
  * This is a source-inventory tripwire, not a proof that an allowlisted consumer has correct historical semantics. In particular, a
  * `FROZEN_GENESIS` row records the current containment boundary, while a `BLOCKED` row records an intentionally absent target consumer.
  * Runtime branch-historical qualification remains covered by KEYREG-006..010 and is not inferred from this suite passing. Likewise, a
  * required-zero-effect entry is an instrumentation obligation; checking that its adapter-negative test name exists does not prove that the
  * test observes each named effect. Direct/dominated evidence columns are hard-coded review metadata bound to that exact test anchor; this
  * suite checks the reviewed mapping but does not parse the test's assertions.
  */
object OperatorConsensusKeySemanticManifestSuite extends SimpleIOSuite {

  private final case class Source(path: String, contents: String)

  private final case class ManifestRow(
    id: String,
    status: String,
    subsystem: String,
    kind: String,
    source: String,
    expectedCount: Int,
    negativeSource: String,
    negativeAnchor: String,
    qualificationSource: String,
    qualificationAnchor: String,
    requiredZeroEffects: String,
    limitation: String,
    directZeroEffects: String,
    dominatedZeroEffects: String
  )

  private final case class ReviewedEvidence(
    qualificationSource: String,
    qualificationAnchor: String,
    directEffects: Set[String],
    dominatedEffects: Set[String]
  )

  private val allowedStatuses = Set("FROZEN_GENESIS", "FAIL_CLOSED_SCAFFOLD", "BLOCKED")

  /** Reviewed K7b evidence is deliberately bounded to the frozen shard-producer and execution-attester adapters. The exact source and test
    * anchor are frozen together with the reviewed effects, so TSV-only anchor substitution cannot preserve a qualification claim. Direct
    * effects have a concrete counter or queried sink in the named test. Producer `Replay` is only the injected derivation-hook counter;
    * scheduled-duty selection is source-dominated by the directly counted zero duty-order call. `EdSign` and outer-envelope signing are
    * control-flow dominated by the directly observed zero KES call; outer signing has no manifest effect label or direct counter, and no
    * production signing injection is added merely to expose one.
    */
  private val shardProducerQualificationSource =
    "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/infrastructure/sharding/ShardCheckpointProducerSuite.scala"
  private val shardProducerIdentityQualificationAnchor =
    "K7b-2: frozen shard-producer identity rejects fresh mint and held re-publish before eta, duty, possession proof, derivation hook, KES signing, or publish"
  private val executionAttesterQualificationSource =
    "modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/infrastructure/sharding/ShardCheckpointAttestationEmitterSuite.scala"
  private val executionIdentityQualificationAnchor =
    "K7b: frozen execution-attester identity rejects before eta, proof, signing, verification, tracking, or publish after one concrete-manager re-execution-hook invocation"
  private val localVerificationQualificationAnchor =
    "reject: an unverified local execution signature is neither recorded nor published"

  private val reviewedQualifiedEvidence: Map[String, ReviewedEvidence] = Map(
    "KSEM-EXEC-003" -> ReviewedEvidence(
      shardProducerQualificationSource,
      shardProducerIdentityQualificationAnchor,
      Set("EtaLookup", "Draw", "PossessionProof", "Replay", "KesSign", "Publish"),
      Set("EdSign")
    ),
    "KSEM-EXEC-004" -> ReviewedEvidence(
      shardProducerQualificationSource,
      shardProducerIdentityQualificationAnchor,
      Set("EtaLookup", "Draw", "PossessionProof", "Replay", "KesSign", "Publish"),
      Set("EdSign")
    ),
    "KSEM-EXEC-005" -> ReviewedEvidence(
      shardProducerQualificationSource,
      shardProducerIdentityQualificationAnchor,
      Set("EtaLookup", "Draw", "PossessionProof", "Replay", "KesSign", "Publish"),
      Set("EdSign")
    ),
    "KSEM-EXEC-006" -> ReviewedEvidence(
      shardProducerQualificationSource,
      shardProducerIdentityQualificationAnchor,
      Set("EtaLookup", "Draw", "PossessionProof", "Replay", "KesSign", "Publish"),
      Set("EdSign")
    ),
    "KSEM-EXEC-007" -> ReviewedEvidence(
      shardProducerQualificationSource,
      shardProducerIdentityQualificationAnchor,
      Set("EtaLookup", "Draw", "PossessionProof", "Replay", "KesSign", "Publish"),
      Set("EdSign")
    ),
    "KSEM-EXEC-008" -> ReviewedEvidence(
      executionAttesterQualificationSource,
      executionIdentityQualificationAnchor,
      Set("EtaLookup", "PossessionProof", "KesSign", "TrackerRecord", "Publish"),
      Set("EdSign")
    ),
    "KSEM-EXEC-009" -> ReviewedEvidence(
      executionAttesterQualificationSource,
      executionIdentityQualificationAnchor,
      Set("EtaLookup", "PossessionProof", "KesSign", "TrackerRecord", "Publish"),
      Set("EdSign")
    ),
    "KSEM-EXEC-010" -> ReviewedEvidence(
      executionAttesterQualificationSource,
      executionIdentityQualificationAnchor,
      Set("EtaLookup", "PossessionProof", "KesSign", "TrackerRecord", "Publish"),
      Set("EdSign")
    ),
    "KSEM-EXEC-011" -> ReviewedEvidence(
      executionAttesterQualificationSource,
      localVerificationQualificationAnchor,
      Set("TrackerRecord", "Publish"),
      Set.empty
    )
  )

  /** These are semantic API calls, not primitive names. A consumer remains visible even when it delegates VRF/KES work through a helper and
    * never mentions `EcVrf25519` or `OperationalKeyMaker` directly.
    */
  private val callKinds: Map[String, Regex] = Map(
    "active-pair-resolve" -> raw"\bActiveOperatorConsensusKeys\s*\.\s*resolve\s*\(".r,
    "active-pair-validity" -> raw"\bActiveOperatorConsensusKeys\s*\.\s*isValidAt\s*\(".r,
    "active-pair-kes-step" -> raw"\bActiveOperatorConsensusKeys\s*\.\s*treeStep\s*\(".r,
    "local-key-material-load" -> raw"\bLocalOperatorKeyPairGate\s*\.\s*loadVerified(?:\s*\[[^\]]+\])?\s*\(".r,
    "local-signing-capability" -> raw"\bLocalOperatorKeyPairGate\s*\.\s*registeredSigningKey\s*\(".r,
    "local-resolved-signing-capability" -> raw"\bLocalOperatorKeyPairGate\s*\.\s*signingKeyFromResolved\s*\(".r,
    "local-vrf-derive" -> raw"\bSnapshotLeaderLoop\s*\.\s*deriveVrfKeys\s*\(".r,
    "gl0-snapshot-pair-resolve" -> raw"\bNakamotoSnapshotValidator\s*\.\s*resolveRegisteredOperatorKeys\s*\(".r,
    "gl0-snapshot-kes-verify" -> raw"\bKesGossipVerification\s*\.\s*verifySnapshot\s*\(".r,
    "optimistic-kes-verify" -> raw"\bKesGossipVerification\s*\.\s*verifyAttestation\s*\(".r,
    "admission-kes-by-step-verify" -> raw"\bKesGossipVerification\s*\.\s*verifyAttestationByStep(?:\s*\[[^\]]+\])?\s*\(".r,
    "leader-vrf-draw" -> raw"\.\s*checkEligibility\s*\(".r,
    "leader-vrf-verify" -> raw"\.\s*verifyEligibility\s*\(".r,
    "admission-vrf-draw" -> raw"\.\s*isInCommittee\s*\(".r,
    "admission-vrf-verify" -> raw"\.\s*verifyMembership(?:Detailed)?\s*\(".r,
    "execution-vk-draw" -> raw"\bCommitteeSortition\s*\.\s*isInShardCommittee(?:\s*\[[^\]]+\])?\s*\(".r,
    "shard-possession-prove" -> raw"\.\s*(?:membershipProof|vrfProofForSlot)\s*\(".r,
    "shard-duty-order" -> raw"\b(?:slotLeader|ShardSlotLeader)\s*\.\s*dutyOrder(?:\s*\[[^\]]+\])?\s*\(".r,
    "shard-duty-select" -> raw"\bShardSlotLeader\s*\.\s*scheduledDuty\s*\(".r,
    "kes-sign" -> raw"\bkesSigner\s*\.\s*sign\s*\(".r,
    "kes-verify" -> raw"\bkesVerifier\s*\.\s*verify\s*\(".r,
    "checkpoint-certificate-verify" -> raw"\.\s*verifyCommitteeSignature\s*\(".r,
    "checkpoint-certificate-local-verify" -> raw"\bsignatureVerification\s*<-\s*verifyCommitteeSignature\s*\(".r,
    "atomic-registry-get" -> raw"\boperatorKeyRegistry\s*\.\s*get\s*\(".r,
    "slashing-key-resolve" -> raw"\bkeyResolver\s*\.\s*resolve\s*\(".r,
    "tower-proof-verify" -> raw"\bverifier\s*\.\s*verify\s*\(\s*proof\b".r
  )

  /** Planned consumers must stay absent until their policy and historical authority inputs are implemented. These broad terms are a
    * secondary tripwire; any future use of the semantic APIs above is caught by `callKinds` regardless of naming.
    */
  private val blockedKinds: Map[String, Regex] = Map(
    "blocked-watchtower-assignment" -> raw"\b(?:select|assign|draw|sample)Watchtowers?\s*\(".r,
    "blocked-optimistic-vrf-sampling" -> raw"\b(?:OptimisticVrfSampler|FinalityVrfSampler|sampleOptimisticCommittee)\b".r
  )

  test("semantic manifest matches every reviewed higher-level consumer spelling and file count") {
    (manifestRows, productionSources).mapN { (rows, sources) =>
      val actual = semanticInventory(sources)

      val activeRows = rows.filterNot(_.status == "BLOCKED")
      val expected = activeRows.map(row => (row.kind, row.source) -> row.expectedCount).toMap
      val duplicateKeys = activeRows
        .groupBy(row => row.kind -> row.source)
        .collect { case (key, matches) if matches.sizeCompare(1) > 0 => key }
        .toList
        .sorted
      val unknownKinds = activeRows.iterator.map(_.kind).toSet -- callKinds.keySet
      val unmanifested = actual.keySet -- expected.keySet
      val stale = expected.keySet -- actual.keySet
      val changed = actual.keySet.intersect(expected.keySet).toList.sorted.collect {
        case key if actual(key) != expected(key) => s"${key._1}@${key._2} expected=${expected(key)} actual=${actual(key)}"
      }

      expect
        .all(
          duplicateKeys.isEmpty,
          unknownKinds.isEmpty,
          unmanifested.isEmpty,
          stale.isEmpty,
          changed.isEmpty
        )
        .and(
          if (duplicateKeys.isEmpty && unknownKinds.isEmpty && unmanifested.isEmpty && stale.isEmpty && changed.isEmpty) success
          else
            failure(
              s"duplicate=${duplicateKeys.mkString(",")} unknownKinds=${unknownKinds.toList.sorted.mkString(",")} " +
                s"unmanifested=${unmanifested.toList.sorted.mkString(",")} stale=${stale.toList.sorted.mkString(",")} " +
                s"changed=${changed.mkString(",")}"
            )
        )
    }
  }

  pureTest("semantic scanner exposes an added higher-level consumer before it can be allowlisted accidentally") {
    val synthetic = Source(
      "modules/example/src/main/scala/Unreviewed.scala",
      "object Unreviewed { val result = eligibilityChecker.checkEligibility(key, slot, gap, eta, stake, config) }"
    )
    val actual = semanticInventory(List(synthetic))
    expect.same(Map(("leader-vrf-draw", synthetic.path) -> 1), actual)
  }

  pureTest("semantic scanner ignores consumer spellings in comments and literals") {
    val synthetic = Source(
      "modules/example/src/main/scala/Spoofed.scala",
      List(
        "// eligibilityChecker.checkEligibility(key, slot, gap, eta, stake, config)",
        "/* eligibilityChecker.checkEligibility(key, slot, gap, eta, stake, config) */",
        "val normal = \"eligibilityChecker.checkEligibility(key, slot, gap, eta, stake, config)\"",
        "val triple = \"\"\"eligibilityChecker.checkEligibility(key, slot, gap, eta, stake, config)\"\"\"",
        "val character = '('",
        "val untouchedCode = 1"
      ).mkString("\n")
    )

    expect.same(Map.empty[(String, String), Int], semanticInventory(List(synthetic)))
  }

  pureTest("interpolated-literal tripwire exposes reviewed semantic call spellings") {
    val interpolation = "$" + "{eligibilityChecker.checkEligibility(key, slot, gap, eta, stake, config)}"
    val synthetic = Source(
      "modules/example/src/main/scala/Interpolated.scala",
      "val hidden = s\"" + interpolation + "\""
    )

    expect.same(Map(("leader-vrf-draw", synthetic.path) -> 1), interpolatedSemanticInventory(List(synthetic)))
  }

  test("production interpolated literals contain no reviewed semantic call spelling") {
    productionSources.map { sources =>
      val hidden = interpolatedSemanticInventory(sources)
      if (hidden.isEmpty) success else failure(s"semantic calls hidden in interpolated literals=$hidden")
    }
  }

  test("blocked target consumers remain absent rather than silently acquiring local authority") {
    (manifestRows, productionSources).mapN { (rows, sources) =>
      val blockedRows = rows.filter(_.status == "BLOCKED")
      val expectedKinds = blockedKinds.keySet
      val manifestKinds = blockedRows.iterator.map(_.kind).toSet
      val invalidShape = blockedRows.filter(row => row.source != "-" || row.expectedCount != 0)
      val occurrences = blockedKinds.toList.flatMap {
        case (kind, pattern) =>
          sources.flatMap(source => Option.when(pattern.findFirstIn(scalaCodeOnly(source.contents)).nonEmpty)(kind -> source.path))
      }

      expect
        .all(
          manifestKinds == expectedKinds,
          invalidShape.isEmpty,
          occurrences.isEmpty
        )
        .and(
          if (manifestKinds == expectedKinds && invalidShape.isEmpty && occurrences.isEmpty) success
          else
            failure(
              s"manifestKinds=${manifestKinds.toList.sorted.mkString(",")} expected=${expectedKinds.toList.sorted.mkString(",")} " +
                s"invalidShape=${invalidShape.map(_.id).sorted.mkString(",")} occurrences=${occurrences.sorted.mkString(",")}"
            )
        )
    }
  }

  test("every manifest row has an uncommented and unquoted negative-vector test-shaped declaration") {
    manifestRows.flatMap { rows =>
      rows.traverse { row =>
        IO.blocking {
          val root = repositoryRoot(Paths.get(sys.props("user.dir")).toAbsolutePath.normalize())
          val path = root.resolve(row.negativeSource).normalize()
          val insideRoot = path.startsWith(root)
          val normalized = path.toString.replace('\\', '/')
          val isTestSource = normalized.contains("/src/test/scala/") && normalized.endsWith(".scala")
          val contents = Option.when(insideRoot && isTestSource && Files.isRegularFile(path))(
            new String(Files.readAllBytes(path), StandardCharsets.UTF_8)
          )
          val found = contents.exists(source => executableTestAnchors(source).contains(row.negativeAnchor))
          (row.id, insideRoot, isTestSource, Files.isRegularFile(path), found)
        }
      }.map { results =>
        val failures = results.collect {
          case (id, inside, testSource, regular, found) if !inside || !testSource || !regular || !found =>
            s"$id(inside=$inside testSource=$testSource regular=$regular anchor=$found)"
        }
        if (failures.isEmpty) success else failure(s"missing/renamed negative-vector test declarations=${failures.mkString(",")}")
      }
    }
  }

  test("every row records bounded reviewed zero-effect metadata beside its adapter obligation") {
    manifestRows.flatMap { rows =>
      rows
        .filterNot(_.status == "BLOCKED")
        .traverse { row =>
          IO.blocking {
            val root = repositoryRoot(Paths.get(sys.props("user.dir")).toAbsolutePath.normalize())
            val path = root.resolve(row.qualificationSource).normalize()
            val insideRoot = path.startsWith(root)
            val normalized = path.toString.replace('\\', '/')
            val isTestSource = normalized.contains("/src/test/scala/") && normalized.endsWith(".scala")
            val contents = Option.when(insideRoot && isTestSource && Files.isRegularFile(path))(
              new String(Files.readAllBytes(path), StandardCharsets.UTF_8)
            )
            val anchorFound = contents.exists(source => executableTestAnchors(source).contains(row.qualificationAnchor))
            val requiredEffects = parseEffects(row.requiredZeroEffects)
            val directEffects = parseEffects(row.directZeroEffects)
            val dominatedEffects = parseEffects(row.dominatedZeroEffects)
            val allDeclaredEffects = requiredEffects ++ directEffects ++ dominatedEffects
            val distinctEffects =
              List(requiredEffects, directEffects, dominatedEffects).forall(values => values.distinct.size == values.size)
            val unknownEffects = allDeclaredEffects.toSet -- OperatorConsensusKeyQualificationMatrix.EffectObligation.byManifestName.keySet
            val evidenceIsRequired = (directEffects.toSet ++ dominatedEffects.toSet).subsetOf(requiredEffects.toSet)
            val evidenceKindsDisjoint = directEffects.toSet.intersect(dominatedEffects.toSet).isEmpty
            (
              row.id,
              insideRoot,
              isTestSource,
              Files.isRegularFile(path),
              anchorFound,
              distinctEffects,
              unknownEffects,
              evidenceIsRequired,
              evidenceKindsDisjoint
            )
          }
        }
        .map { results =>
          val failures = results.collect {
            case (id, inside, testSource, regular, anchor, distinct, unknown, required, disjoint)
                if !inside || !testSource || !regular || !anchor || !distinct || unknown.nonEmpty || !required || !disjoint =>
              s"$id(inside=$inside testSource=$testSource regular=$regular anchor=$anchor distinct=$distinct " +
                s"unknown=${unknown.toList.sorted.mkString("|")} evidenceRequired=$required evidenceDisjoint=$disjoint)"
          }
          val coveredEffects = rows
            .filterNot(_.status == "BLOCKED")
            .flatMap(row => parseEffects(row.requiredZeroEffects))
            .toSet
          val expectedEffects = OperatorConsensusKeyQualificationMatrix.EffectObligation.byManifestName.keySet
          val actualQualifiedEvidence = qualifiedEvidence(rows)

          if (
            failures.isEmpty &&
            coveredEffects == expectedEffects &&
            actualQualifiedEvidence == reviewedQualifiedEvidence
          ) success
          else
            failure(
              s"invalid adapter obligation mappings=${failures.mkString(",")} " +
                s"covered=${coveredEffects.toList.sorted.mkString("|")} expected=${expectedEffects.toList.sorted.mkString("|")} " +
                s"qualified=$actualQualifiedEvidence reviewed=$reviewedQualifiedEvidence"
            )
        }
    }
  }

  test("reviewed evidence binding rejects an EXEC-008 to EXEC-011 qualification-anchor swap") {
    manifestRows.map { rows =>
      val baseline = qualifiedEvidence(rows)
      val swapped = qualifiedEvidence(
        rows.map {
          case row if row.id == "KSEM-EXEC-008" =>
            row.copy(qualificationAnchor = localVerificationQualificationAnchor)
          case row => row
        }
      )

      expect.all(
        baseline == reviewedQualifiedEvidence,
        swapped != reviewedQualifiedEvidence,
        swapped.get("KSEM-EXEC-008").exists(_.qualificationAnchor == localVerificationQualificationAnchor)
      )
    }
  }

  test("reviewed evidence binding rejects K7b-2 producer source, anchor, and effect mutations") {
    manifestRows.map { rows =>
      val baseline = qualifiedEvidence(rows)
      val sourceMutated = qualifiedEvidence(
        rows.map {
          case row if row.id == "KSEM-EXEC-003" =>
            row.copy(qualificationSource = executionAttesterQualificationSource)
          case row => row
        }
      )
      val anchorMutated = qualifiedEvidence(
        rows.map {
          case row if row.id == "KSEM-EXEC-004" =>
            row.copy(qualificationAnchor = executionIdentityQualificationAnchor)
          case row => row
        }
      )
      val effectMutated = qualifiedEvidence(
        rows.map {
          case row if row.id == "KSEM-EXEC-005" =>
            row.copy(directZeroEffects = "EtaLookup|Draw|PossessionProof|KesSign|Publish")
          case row => row
        }
      )

      expect.all(
        baseline == reviewedQualifiedEvidence,
        sourceMutated != reviewedQualifiedEvidence,
        anchorMutated != reviewedQualifiedEvidence,
        effectMutated != reviewedQualifiedEvidence
      )
    }
  }

  pureTest("negative-vector lexer accepts test-shaped declarations and rejects comment or literal spoofs") {
    val source = List(
      "test(\"live IO vector\") { IO.unit }",
      "pureTest ( \"live pure vector\" ) { success }",
      "// test(\"line-comment spoof\") { success }",
      "/* pureTest(\"block-comment spoof\") { success } */",
      "val normal = \"test(\\\"normal-string spoof\\\")\"",
      "val triple = \"\"\"pureTest(\"triple-string spoof\")\"\"\"",
      "val character = 't'"
    ).mkString("\n")

    expect.same(Set("live IO vector", "live pure vector"), executableTestAnchors(source))
  }

  test("manifest schema is bounded and explicitly non-proof") {
    manifestRows.map { rows =>
      val duplicateIds = rows.groupBy(_.id).collect { case (id, matches) if matches.sizeCompare(1) > 0 => id }.toList.sorted
      val badStatuses = rows.iterator.map(_.status).toSet -- allowedStatuses
      val emptyFields = rows.collect {
        case row
            if List(
              row.id,
              row.status,
              row.subsystem,
              row.kind,
              row.source,
              row.negativeSource,
              row.negativeAnchor,
              row.qualificationSource,
              row.qualificationAnchor,
              row.requiredZeroEffects,
              row.limitation,
              row.directZeroEffects,
              row.dominatedZeroEffects
            )
              .exists(_.trim.isEmpty) =>
          row.id
      }
      val misleadingLimitations = rows.collect {
        case row if !row.limitation.toLowerCase.contains("not qualified") => row.id
      }
      val badBlockedQualificationShape = rows.collect {
        case row
            if row.status == "BLOCKED" &&
              (row.qualificationSource != "-" ||
                row.qualificationAnchor != "-" ||
                row.requiredZeroEffects != "-" ||
                row.directZeroEffects != "-" ||
                row.dominatedZeroEffects != "-") =>
          row.id
      }
      val badActiveQualificationShape = rows.collect {
        case row
            if row.status != "BLOCKED" &&
              (row.qualificationSource == "-" || row.qualificationAnchor == "-" || row.requiredZeroEffects == "-") =>
          row.id
      }

      expect.all(
        duplicateIds.isEmpty,
        badStatuses.isEmpty,
        emptyFields.isEmpty,
        misleadingLimitations.isEmpty,
        badBlockedQualificationShape.isEmpty,
        badActiveQualificationShape.isEmpty
      )
    }
  }

  private def manifestRows: IO[List[ManifestRow]] =
    IO.blocking {
      val root = repositoryRoot(Paths.get(sys.props("user.dir")).toAbsolutePath.normalize())
      val path = root.resolve("docs/review/OPERATOR-CONSENSUS-KEY-SEMANTIC-MANIFEST.tsv")
      val lines = Files.readAllLines(path, StandardCharsets.UTF_8).asScala.toList
      val expectedHeader =
        "id\tstatus\tsubsystem\tkind\tsource\texpected_count\tnegative_source\tnegative_anchor\tqualification_source\tqualification_anchor\trequired_zero_effects\tlimitation\tdirect_zero_effects\tdominated_zero_effects"
      require(lines.headOption.contains(expectedHeader), s"Unexpected semantic manifest header in $path")
      lines.drop(1).filter(_.trim.nonEmpty).map { line =>
        val fields = line.split("\t", -1).toList
        require(fields.size == 14, s"Expected 14 tab-separated fields, got ${fields.size}: $line")
        val id :: status :: subsystem :: kind :: source :: expected :: negativeSource :: negativeAnchor :: qualificationSource :: qualificationAnchor :: requiredZeroEffects :: limitation :: directZeroEffects :: dominatedZeroEffects :: Nil =
          fields
        val expectedCount = Try(expected.toInt).getOrElse(throw new IllegalArgumentException(s"Invalid expected_count for $id: $expected"))
        ManifestRow(
          id,
          status,
          subsystem,
          kind,
          source,
          expectedCount,
          negativeSource,
          negativeAnchor,
          qualificationSource,
          qualificationAnchor,
          requiredZeroEffects,
          limitation,
          directZeroEffects,
          dominatedZeroEffects
        )
      }
    }

  private def parseEffects(value: String): List[String] =
    if (value == "-") Nil else value.split("\\|", -1).toList

  private def qualifiedEvidence(rows: List[ManifestRow]): Map[String, ReviewedEvidence] =
    rows.flatMap { row =>
      val direct = parseEffects(row.directZeroEffects).toSet
      val dominated = parseEffects(row.dominatedZeroEffects).toSet
      Option.when(direct.nonEmpty || dominated.nonEmpty)(
        row.id -> ReviewedEvidence(
          row.qualificationSource,
          row.qualificationAnchor,
          direct,
          dominated
        )
      )
    }.toMap

  private def productionSources: IO[List[Source]] =
    IO.blocking {
      val root = repositoryRoot(Paths.get(sys.props("user.dir")).toAbsolutePath.normalize())
      val stream = Files.walk(root.resolve("modules"))
      try
        stream
          .iterator()
          .asScala
          .filter(Files.isRegularFile(_))
          .filter(_.getFileName.toString.endsWith(".scala"))
          .filter(_.toString.replace('\\', '/').contains("/src/main/scala/"))
          .map { path =>
            Source(root.relativize(path).toString.replace('\\', '/'), new String(Files.readAllBytes(path), StandardCharsets.UTF_8))
          }
          .toList
      finally stream.close()
    }

  private def semanticInventory(sources: List[Source]): Map[(String, String), Int] =
    callKinds.toList.flatMap {
      case (kind, pattern) =>
        sources.flatMap { source =>
          val count = pattern.findAllIn(scalaCodeOnly(source.contents)).size
          Option.when(count > 0)((kind, source.path) -> count)
        }
    }.toMap

  /** Fail-closed companion to `scalaCodeOnly`: reviewed call spellings inside any interpolated literal require manual review. */
  private def interpolatedSemanticInventory(sources: List[Source]): Map[(String, String), Int] =
    callKinds.toList.flatMap {
      case (kind, pattern) =>
        sources.flatMap { source =>
          val count = interpolatedLiteralBodies(source.contents).iterator.map(pattern.findAllIn(_).size).sum
          Option.when(count > 0)((kind, source.path) -> count)
        }
    }.toMap

  /** Returns raw bodies of lexically recognized Scala interpolated literals while ignoring comments and ordinary literals. */
  private def interpolatedLiteralBodies(input: String): List[String] = {
    val bodies = List.newBuilder[String]
    var index = 0
    var blockDepth = 0
    var inLineComment = false

    def isIdentifierPart(char: Char): Boolean = char.isLetterOrDigit || char == '_' || char == '$'

    def hasInterpolator(quote: Int): Boolean = {
      var cursor = quote - 1
      while (cursor >= 0 && isIdentifierPart(input.charAt(cursor))) cursor -= 1
      cursor < quote - 1
    }

    def readString(from: Int): (String, Int) = {
      val triple = input.startsWith("\"\"\"", from)
      val start = from + (if (triple) 3 else 1)
      var cursor = start
      var escaped = false
      var closedAt = -1

      while (cursor < input.length && closedAt < 0)
        if (triple && input.startsWith("\"\"\"", cursor)) closedAt = cursor
        else {
          val char = input.charAt(cursor)
          if (!triple && !escaped && char == '"') closedAt = cursor
          else {
            if (!triple && escaped) escaped = false
            else if (!triple && char == '\\') escaped = true
            cursor += 1
          }
        }

      if (closedAt < 0) input.substring(start) -> input.length
      else input.substring(start, closedAt) -> (closedAt + (if (triple) 3 else 1))
    }

    def skipCharacter(from: Int): Int = {
      var cursor = from + 1
      var escaped = false
      while (cursor < input.length) {
        val char = input.charAt(cursor)
        cursor += 1
        if (escaped) escaped = false
        else if (char == '\\') escaped = true
        else if (char == '\'') return cursor
      }
      cursor
    }

    while (index < input.length) {
      val current = input.charAt(index)
      val next = if (index + 1 < input.length) input.charAt(index + 1) else '\u0000'

      if (inLineComment) {
        if (current == '\n') inLineComment = false
        index += 1
      } else if (blockDepth > 0) {
        if (current == '/' && next == '*') {
          blockDepth += 1
          index += 2
        } else if (current == '*' && next == '/') {
          blockDepth -= 1
          index += 2
        } else index += 1
      } else if (current == '/' && next == '/') {
        inLineComment = true
        index += 2
      } else if (current == '/' && next == '*') {
        blockDepth = 1
        index += 2
      } else if (current == '"') {
        val (body, end) = readString(index)
        if (hasInterpolator(index)) bodies += body
        index = end
      } else if (current == '\'') index = skipCharacter(index)
      else index += 1
    }

    bodies.result()
  }

  /** Retains only Scala code, replacing comments and literals with whitespace while preserving newlines and token separation. */
  private def scalaCodeOnly(input: String): String = {
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
        } else out.append(' ')
        index += 1
      } else if (blockDepth > 0) {
        if (current == '/' && next == '*') {
          out.append("  ")
          blockDepth += 1
          index += 2
        } else if (current == '*' && next == '/') {
          out.append("  ")
          blockDepth -= 1
          index += 2
        } else {
          out.append(if (current == '\n') '\n' else ' ')
          index += 1
        }
      } else if (inTripleString) {
        if (current == '"' && input.startsWith("\"\"\"", index)) {
          out.append("   ")
          index += 3
          inTripleString = false
        } else {
          out.append(if (current == '\n') '\n' else ' ')
          index += 1
        }
      } else if (inString) {
        out.append(if (current == '\n') '\n' else ' ')
        index += 1
        if (escaped) escaped = false
        else if (current == '\\') escaped = true
        else if (current == '"') inString = false
      } else if (inCharacter) {
        out.append(if (current == '\n') '\n' else ' ')
        index += 1
        if (escaped) escaped = false
        else if (current == '\\') escaped = true
        else if (current == '\'') inCharacter = false
      } else if (current == '/' && next == '/') {
        out.append("  ")
        inLineComment = true
        index += 2
      } else if (current == '/' && next == '*') {
        out.append("  ")
        blockDepth = 1
        index += 2
      } else if (current == '"' && input.startsWith("\"\"\"", index)) {
        out.append("   ")
        index += 3
        inTripleString = true
      } else {
        if (current == '"') {
          out.append(' ')
          inString = true
        } else if (current == '\'') {
          out.append(' ')
          inCharacter = true
        } else out.append(current)
        index += 1
      }
    }

    out.result()
  }

  /** Extracts lexical `test("name")` and `pureTest("name")` call shapes from code, without attempting symbol resolution. */
  private def executableTestAnchors(input: String): Set[String] = {
    val anchors = Set.newBuilder[String]
    var index = 0
    var blockDepth = 0
    var inLineComment = false

    def isIdentifierPart(char: Char): Boolean = char.isLetterOrDigit || char == '_' || char == '$'

    def skipWhitespace(from: Int): Int = {
      var cursor = from
      while (cursor < input.length && input.charAt(cursor).isWhitespace) cursor += 1
      cursor
    }

    def skipQuoted(from: Int, delimiter: Char): Int = {
      var cursor = from + 1
      var escaped = false
      while (cursor < input.length) {
        val char = input.charAt(cursor)
        cursor += 1
        if (escaped) escaped = false
        else if (char == '\\') escaped = true
        else if (char == delimiter) return cursor
      }
      cursor
    }

    def parseName(from: Int): Option[String] =
      if (from >= input.length || input.charAt(from) != '"' || input.startsWith("\"\"\"", from)) None
      else {
        val name = new StringBuilder
        var cursor = from + 1
        var escaped = false
        var closed = false

        while (cursor < input.length && !closed) {
          val char = input.charAt(cursor)
          cursor += 1
          if (escaped) {
            val decoded = char match {
              case 'b'   => '\b'
              case 'f'   => '\f'
              case 'n'   => '\n'
              case 'r'   => '\r'
              case 't'   => '\t'
              case other => other
            }
            name.append(decoded)
            escaped = false
          } else if (char == '\\') escaped = true
          else if (char == '"') closed = true
          else name.append(char)
        }

        Option.when(closed)(name.result())
      }

    while (index < input.length) {
      val current = input.charAt(index)
      val next = if (index + 1 < input.length) input.charAt(index + 1) else '\u0000'

      if (inLineComment) {
        if (current == '\n') inLineComment = false
        index += 1
      } else if (blockDepth > 0) {
        if (current == '/' && next == '*') {
          blockDepth += 1
          index += 2
        } else if (current == '*' && next == '/') {
          blockDepth -= 1
          index += 2
        } else index += 1
      } else if (current == '/' && next == '/') {
        inLineComment = true
        index += 2
      } else if (current == '/' && next == '*') {
        blockDepth = 1
        index += 2
      } else if (current == '"' && input.startsWith("\"\"\"", index)) {
        val end = input.indexOf("\"\"\"", index + 3)
        index = if (end < 0) input.length else end + 3
      } else if (current == '"') index = skipQuoted(index, '"')
      else if (current == '\'') index = skipQuoted(index, '\'')
      else if (current.isLetter || current == '_') {
        val start = index
        index += 1
        while (index < input.length && isIdentifierPart(input.charAt(index))) index += 1
        val token = input.substring(start, index)
        if (token == "test" || token == "pureTest") {
          val open = skipWhitespace(index)
          if (open < input.length && input.charAt(open) == '(') {
            val nameStart = skipWhitespace(open + 1)
            parseName(nameStart).foreach(anchors += _)
          }
        }
      } else index += 1
    }

    anchors.result()
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
