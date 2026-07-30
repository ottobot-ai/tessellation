package io.constellationnetwork.node.shared.domain.nakamoto

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import cats.effect.IO
import cats.syntax.all._

import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.util.matching.Regex

import weaver.SimpleIOSuite

/** Source-backed review tripwire for receiver-local consensus and economic authority.
  *
  * Passing this suite does not prove that the manifest is semantically complete and does not make any local configuration safe. It freezes
  * the reviewed current surfaces, makes every `NAKAMOTO_*` HOCON override visible, and rejects a new production Scala or Go file that
  * combines a raw environment API with a Nakamoto key until its authority and target are classified. This is deliberately a conservative
  * lexical tripwire, not a whole-program data-flow proof: a generic environment wrapper and its key in different source files require
  * manual review.
  */
object ConsensusConfigAuthorityInventorySuite extends SimpleIOSuite {

  private val expectedRowCount = 102

  private val requiredNodeLocalConsensusRows = Set(
    "CFG-GL0-012",
    "CFG-GL0-013",
    "CFG-GL0-014",
    "CFG-GL0-015",
    "CFG-GL0-016",
    "CFG-GL0-017",
    "CFG-GL0-018"
  )

  private val requiredProtocolEnvironmentRows = Set(
    "CFG-IDENT-001",
    "CFG-IDENT-002",
    "CFG-IDENT-003",
    "CFG-IDENT-004"
  )

  private final case class Evidence(path: String, line: Int)

  private final case class ManifestRow(
    id: String,
    family: String,
    authorityKey: String,
    currentSource: Evidence,
    sourceMarker: String,
    sourceMarkerCount: Int,
    liveConsumer: Evidence,
    consumerMarker: String,
    consumerMarkerCount: Int,
    effect: String,
    targetAuthority: String,
    status: String
  )

  private final case class Source(path: String, contents: String)

  private val expectedHeader =
    "id\tfamily\tauthority_key\tcurrent_source_evidence\tsource_marker\tsource_marker_count\tlive_consumer_evidence\tconsumer_marker\tconsumer_marker_count\teffect\ttarget_authority\tstatus"

  private val allowedStatuses = Set(
    "OPEN_CONSENSUS_AUTHORITY",
    "OPEN_ROOT_AUTHORITY",
    "OPEN_ROSTER_AUTHORITY",
    "OPEN_RECOVERY_AUTHORITY",
    "DARK_TELEMETRY",
    "LOCAL_QOS_ONLY",
    "GENESIS_NETWORK_INPUT",
    "STALE_UNUSED"
  )

  private val allowedFamilies = Set(
    "NAKAMOTO_HOCON_CONSENSUS",
    "NAKAMOTO_HOCON_ECONOMICS",
    "NAKAMOTO_HOCON_FINALITY",
    "NAKAMOTO_HOCON_GENESIS",
    "NAKAMOTO_HOCON_OBSERVABILITY",
    "NAKAMOTO_HOCON_QOS",
    "NAKAMOTO_HOCON_RECOVERY",
    "NAKAMOTO_HOCON_STALE",
    "BUILTIN_ROSTER_HOCON",
    "DIRECT_NAKAMOTO_ENV",
    "DIRECT_PROTOCOL_ENV",
    "ECONOMIC_POLICY_HOCON",
    "HARDCODED_CONSENSUS_INPUT",
    "HARDCODED_REWARD_POLICY",
    "LEGACY_ERA_HOCON",
    "LOCAL_CLI_AUTHORITY",
    "LOCAL_ROSTER_FILE",
    "ML0_HOCON_CONSENSUS",
    "ML0_HOCON_QOS",
    "ML0_HOCON_STALE",
    "NODE_LOCAL_CLOCK_CONSENSUS",
    "SIDECAR_CLI_QOS",
    "SIDECAR_ENV",
    "STALE_LOCAL_CLOCK"
  )

  private val allowedTargets = Set(
    "ACTIVE_ERA_PROTOCOL_RULE",
    "AUTHENTICATED_PHASE2_GL0_FOLLOW",
    "AUTHENTICATED_SHARD_HISTORY_RECOVERY",
    "AUTHENTICATED_BUILD_IDENTITY",
    "BOUNDED_PROTOCOL_SLOT_CLOCK",
    "GENESIS_COMMITMENT",
    "LOCAL_OBSERVABILITY",
    "LOCAL_QOS",
    "LOCAL_RECOVERY_POLICY",
    "ML0_GENESIS_PARAMETER",
    "REMOVE_UNUSED",
    "ROOTED_CONSENSUS_PARAMETER_OBJECT",
    "ROOTED_DELIVERY_CURSOR_AND_NULLIFIER",
    "ROOTED_ECONOMIC_POLICY",
    "ROOTED_FINALITY_EVIDENCE_PARAMETER",
    "ROOTED_HISTORICAL_OPERATOR_ROSTER",
    "ROOTED_METAGRAPH_REGISTRY",
    "SCODEC_V1_FROM_ORDINAL_ZERO"
  )

  private val hoconNakamotoOverride: Regex =
    "\\$\\{\\?(NAKAMOTO_[A-Z0-9_]+)\\}".r

  private val nakamotoKeyLiteral: Regex =
    "\"(NAKAMOTO_[A-Z0-9_]+)\"".r

  private val protocolIdentityKeyLiteral: Regex =
    "\"(CL_(?:L0_TOKEN_IDENTIFIER|VERSION_HASH|METAGRAPH_VERSION_HASH|JAR_HASH))\"".r

  private val scalaEnvironmentApi: Regex =
    raw"""(?s)\bsys\s*\.\s*env\b|\b(?:java\s*\.\s*lang\s*\.)?System\s*\.\s*getenv\s*\(""".r

  private val goEnvironmentApi: Regex =
    raw"""(?s)\bos\s*\.\s*(?:Getenv|LookupEnv)\s*\(""".r

  test("inventory rows are unique and every cited source marker remains exact") {
    (manifestRows, repositoryRootF).mapN { (rows, root) =>
      val duplicateIds = rows.groupBy(_.id).collect { case (id, matches) if matches.sizeCompare(1) > 0 => id }.toList.sorted
      val missingNodeLocalRows = requiredNodeLocalConsensusRows -- rows.map(_.id).toSet
      val missingProtocolEnvironmentRows = requiredProtocolEnvironmentRows -- rows.map(_.id).toSet
      val badStatuses = rows.collect { case row if !allowedStatuses(row.status) => s"${row.id}:${row.status}" }
      val badFamilies = rows.collect { case row if !allowedFamilies(row.family) => s"${row.id}:${row.family}" }
      val badTargets = rows.collect { case row if !allowedTargets(row.targetAuthority) => s"${row.id}:${row.targetAuthority}" }
      val evidenceErrors = rows.flatMap(row => verifyEvidence(root, row))

      expect
        .all(
          rows.size == expectedRowCount,
          duplicateIds.isEmpty,
          missingNodeLocalRows.isEmpty,
          missingProtocolEnvironmentRows.isEmpty,
          badStatuses.isEmpty,
          badFamilies.isEmpty,
          badTargets.isEmpty,
          evidenceErrors.isEmpty
        )
        .and(
          if (
            rows.size == expectedRowCount &&
            duplicateIds.isEmpty &&
            missingNodeLocalRows.isEmpty &&
            missingProtocolEnvironmentRows.isEmpty &&
            badStatuses.isEmpty &&
            badFamilies.isEmpty &&
            badTargets.isEmpty &&
            evidenceErrors.isEmpty
          ) success
          else
            failure(
              s"rowCount=${rows.size} expected=$expectedRowCount duplicateIds=${duplicateIds.mkString(",")} " +
                s"missingNodeLocalRows=${missingNodeLocalRows.toList.sorted.mkString(",")} badStatuses=${badStatuses.mkString(",")} " +
                s"missingProtocolEnvironmentRows=${missingProtocolEnvironmentRows.toList.sorted.mkString(",")} " +
                s"badFamilies=${badFamilies.mkString(",")} badTargets=${badTargets.mkString(",")} " +
                s"evidenceErrors=${evidenceErrors.mkString("; ")}"
            )
        )
    }
  }

  test("every application.conf NAKAMOTO override has exactly one reviewed row") {
    (manifestRows, repositoryRootF).mapN { (rows, root) =>
      val applicationConf = read(root.resolve("modules/node-shared/src/main/resources/application.conf"))
      val actual = hoconNakamotoOverride.findAllMatchIn(applicationConf).map(_.group(1)).toSet
      val reviewedRows = rows.filter(_.family.startsWith("NAKAMOTO_HOCON_"))
      val duplicateKeys =
        reviewedRows.groupBy(_.authorityKey).collect { case (key, matches) if matches.sizeCompare(1) > 0 => key }.toList.sorted
      val reviewed = reviewedRows.map(_.authorityKey).toSet
      val unreviewed = actual -- reviewed
      val stale = reviewed -- actual

      expect
        .all(duplicateKeys.isEmpty, unreviewed.isEmpty, stale.isEmpty)
        .and(
          if (duplicateKeys.isEmpty && unreviewed.isEmpty && stale.isEmpty) success
          else
            failure(
              s"duplicateKeys=${duplicateKeys.mkString(",")} unreviewed=${unreviewed.toList.sorted.mkString(",")} " +
                s"stale=${stale.toList.sorted.mkString(",")}"
            )
        )
    }
  }

  test("every production Scala file combining a raw environment API and Nakamoto key has reviewed rows") {
    (manifestRows, productionScalaSources).mapN { (rows, sources) =>
      val actual = directEnvInventory(sources)
      val directRows = rows.filter(_.family == "DIRECT_NAKAMOTO_ENV")
      val expectedPairs = directRows.map(row => (row.authorityKey, row.currentSource.path))
      val duplicatePairs =
        expectedPairs.groupBy(identity).collect { case (pair, matches) if matches.sizeCompare(1) > 0 => pair }.toList.sorted
      val expected = expectedPairs.map(_ -> 1).toMap
      val unreviewed = actual.keySet -- expected.keySet
      val stale = expected.keySet -- actual.keySet
      val changed = actual.keySet.intersect(expected.keySet).toList.sorted.collect {
        case key if actual(key) != expected(key) => s"${key._1}@${key._2} expected=${expected(key)} actual=${actual(key)}"
      }

      expect
        .all(duplicatePairs.isEmpty, unreviewed.isEmpty, stale.isEmpty, changed.isEmpty)
        .and(
          if (duplicatePairs.isEmpty && unreviewed.isEmpty && stale.isEmpty && changed.isEmpty) success
          else
            failure(
              s"duplicatePairs=${duplicatePairs.mkString(",")} unreviewed=${unreviewed.toList.sorted.mkString(",")} " +
                s"stale=${stale.toList.sorted.mkString(",")} changed=${changed.mkString(",")}"
            )
        )
    }
  }

  test("reviewed raw protocol identity environment overrides remain explicitly inventoried") {
    (manifestRows, productionScalaSources).mapN { (rows, sources) =>
      val actual = environmentKeyInventory(sources, scalaEnvironmentApi, protocolIdentityKeyLiteral)
      val protocolRows = rows.filter(_.family == "DIRECT_PROTOCOL_ENV")
      val expectedPairs = protocolRows.map(row => (row.authorityKey, row.currentSource.path))
      val duplicatePairs =
        expectedPairs.groupBy(identity).collect { case (pair, matches) if matches.sizeCompare(1) > 0 => pair }.toList.sorted
      val expected = expectedPairs.map(_ -> 1).toMap
      val unreviewed = actual.keySet -- expected.keySet
      val stale = expected.keySet -- actual.keySet

      expect
        .all(duplicatePairs.isEmpty, unreviewed.isEmpty, stale.isEmpty)
        .and(
          if (duplicatePairs.isEmpty && unreviewed.isEmpty && stale.isEmpty) success
          else
            failure(
              s"duplicatePairs=${duplicatePairs.mkString(",")} unreviewed=${unreviewed.toList.sorted.mkString(",")} " +
                s"stale=${stale.toList.sorted.mkString(",")}"
            )
        )
    }
  }

  test("every production Go NAKAMOTO environment read has one reviewed sidecar row") {
    (manifestRows, productionGoSources).mapN { (rows, sources) =>
      val actual = environmentKeyInventory(sources, goEnvironmentApi)
      val sidecarRows = rows.filter(_.family == "SIDECAR_ENV")
      val expectedPairs = sidecarRows.map(row => (row.authorityKey, row.currentSource.path))
      val duplicatePairs =
        expectedPairs.groupBy(identity).collect { case (pair, matches) if matches.sizeCompare(1) > 0 => pair }.toList.sorted
      val expected = expectedPairs.map(_ -> 1).toMap
      val unreviewed = actual.keySet -- expected.keySet
      val stale = expected.keySet -- actual.keySet

      expect
        .all(duplicatePairs.isEmpty, unreviewed.isEmpty, stale.isEmpty)
        .and(
          if (duplicatePairs.isEmpty && unreviewed.isEmpty && stale.isEmpty) success
          else
            failure(
              s"duplicatePairs=${duplicatePairs.mkString(",")} unreviewed=${unreviewed.toList.sorted.mkString(",")} " +
                s"stale=${stale.toList.sorted.mkString(",")}"
            )
        )
    }
  }

  pureTest("environment scanner exposes aliased and whitespace-separated reads and ignores comments") {
    val scalaSource = Source(
      "modules/example/src/main/scala/Unreviewed.scala",
      List(
        "// sys.env.get(\"NAKAMOTO_COMMENT_ONLY\")",
        "/* sys.env.get(\"NAKAMOTO_BLOCK_COMMENT_ONLY\") */",
        "object Unreviewed {",
        "  val key = \"NAKAMOTO_UNREVIEWED\"",
        "  val unsafe = sys . env",
        "    .get(key)",
        "}"
      ).mkString("\n")
    )
    val javaSource = Source(
      "modules/example/src/main/scala/JavaEnvironment.scala",
      """object JavaEnvironment { val unsafe = System.getenv("NAKAMOTO_SYSTEM_UNREVIEWED") }"""
    )
    val stringOnly = Source(
      "modules/example/src/main/scala/StringOnly.scala",
      """object StringOnly { val key = "NAKAMOTO_NOT_AN_ENV_READ" }"""
    )

    expect.same(
      Map(
        ("NAKAMOTO_UNREVIEWED", scalaSource.path) -> 1,
        ("NAKAMOTO_SYSTEM_UNREVIEWED", javaSource.path) -> 1
      ),
      directEnvInventory(List(scalaSource, javaSource, stringOnly))
    )
  }

  private def verifyEvidence(root: Path, row: ManifestRow): List[String] =
    verifyAnchor(root, row.id, "source", row.currentSource, row.sourceMarker, row.sourceMarkerCount) ++
      verifyAnchor(root, row.id, "consumer", row.liveConsumer, row.consumerMarker, row.consumerMarkerCount)

  private def verifyAnchor(
    root: Path,
    id: String,
    kind: String,
    evidence: Evidence,
    marker: String,
    expectedCount: Int
  ): List[String] = {
    val path = root.resolve(evidence.path).normalize()
    if (!path.startsWith(root))
      List(s"$id $kind path escapes repository: ${evidence.path}")
    else if (!Files.isRegularFile(path))
      List(s"$id $kind path missing: ${evidence.path}")
    else {
      val contents = read(path)
      val lines = contents.linesIterator.toVector
      val badLine =
        evidence.line <= 0 || evidence.line > lines.size || !lines(evidence.line - 1).contains(marker)
      val actualCount = exactCount(contents, marker)
      List(
        Option.when(badLine)(
          s"$id $kind marker absent at ${evidence.path}:${evidence.line}: $marker"
        ),
        Option.when(expectedCount <= 0 || actualCount != expectedCount)(
          s"$id $kind count marker=$marker expected=$expectedCount actual=$actualCount"
        )
      ).flatten
    }
  }

  private def exactCount(contents: String, marker: String): Int = {
    require(marker.nonEmpty, "Inventory markers must be nonempty")
    Iterator
      .iterate(contents.indexOf(marker)) { previous =>
        if (previous < 0) -1 else contents.indexOf(marker, previous + marker.length)
      }
      .takeWhile(_ >= 0)
      .size
  }

  private def directEnvInventory(sources: List[Source]): Map[(String, String), Int] =
    environmentKeyInventory(sources, scalaEnvironmentApi)

  private def environmentKeyInventory(
    sources: List[Source],
    environmentApi: Regex,
    keyLiteral: Regex = nakamotoKeyLiteral
  ): Map[(String, String), Int] =
    sources.flatMap { source =>
      val withoutSourceComments = withoutComments(source.contents)
      if (environmentApi.findFirstIn(withoutSourceComments).isEmpty) List.empty
      else
        keyLiteral
          .findAllMatchIn(withoutSourceComments)
          .map(matched => (matched.group(1), source.path))
          .toSet
          .toList
    }
      .groupMapReduce(identity)(_ => 1)(_ + _)

  /** Removes line and nested block comments while retaining string contents needed to read the environment key literal. */
  private def withoutComments(input: String): String = {
    val output = new StringBuilder(input.length)
    var index = 0
    var blockDepth = 0
    var inLineComment = false
    var inString = false
    var inTripleString = false
    var inCharacter = false
    var escaped = false

    def blank(char: Char): Unit =
      output.append(if (char == '\n' || char == '\r') char else ' ')

    while (index < input.length) {
      val char = input.charAt(index)
      val next = if (index + 1 < input.length) input.charAt(index + 1) else 0.toChar

      if (inLineComment) {
        blank(char)
        if (char == '\n') inLineComment = false
        index += 1
      } else if (blockDepth > 0) {
        if (char == '/' && next == '*') {
          blank(char)
          blank(next)
          blockDepth += 1
          index += 2
        } else if (char == '*' && next == '/') {
          blank(char)
          blank(next)
          blockDepth -= 1
          index += 2
        } else {
          blank(char)
          index += 1
        }
      } else if (inTripleString) {
        if (input.startsWith("\"\"\"", index)) {
          output.append("\"\"\"")
          inTripleString = false
          index += 3
        } else {
          output.append(char)
          index += 1
        }
      } else if (inString) {
        output.append(char)
        if (escaped) escaped = false
        else if (char == '\\') escaped = true
        else if (char == '"') inString = false
        index += 1
      } else if (inCharacter) {
        output.append(char)
        if (escaped) escaped = false
        else if (char == '\\') escaped = true
        else if (char == '\'') inCharacter = false
        index += 1
      } else if (char == '/' && next == '/') {
        blank(char)
        blank(next)
        inLineComment = true
        index += 2
      } else if (char == '/' && next == '*') {
        blank(char)
        blank(next)
        blockDepth = 1
        index += 2
      } else if (input.startsWith("\"\"\"", index)) {
        output.append("\"\"\"")
        inTripleString = true
        index += 3
      } else if (char == '"') {
        output.append(char)
        inString = true
        index += 1
      } else if (char == '\'') {
        output.append(char)
        inCharacter = true
        index += 1
      } else {
        output.append(char)
        index += 1
      }
    }

    output.result()
  }

  private def manifestRows: IO[List[ManifestRow]] =
    repositoryRootF.map { root =>
      val path = root.resolve("docs/review/CONSENSUS-CONFIG-AUTHORITY-INVENTORY.tsv")
      val lines = Files.readAllLines(path, StandardCharsets.UTF_8).asScala.toList
      require(lines.headOption.contains(expectedHeader), s"Unexpected config authority inventory header in $path")
      lines.drop(1).filter(_.trim.nonEmpty).map { line =>
        val fields = line.split("\t", -1).toList
        require(fields.size == 12, s"Expected 12 tab-separated fields got ${fields.size}: $line")
        val id :: family :: authorityKey :: source :: sourceMarker :: sourceCount :: consumer :: consumerMarker :: consumerCount :: effect :: targetAuthority :: status :: Nil =
          fields
        val values =
          List(
            id,
            family,
            authorityKey,
            source,
            sourceMarker,
            sourceCount,
            consumer,
            consumerMarker,
            consumerCount,
            effect,
            targetAuthority,
            status
          )
        require(values.forall(_.trim.nonEmpty), s"Inventory row contains an empty field: $id")
        ManifestRow(
          id,
          family,
          authorityKey,
          parseEvidence(source, id, "current_source_evidence"),
          sourceMarker,
          parsePositiveInt(sourceCount, id, "source_marker_count"),
          parseEvidence(consumer, id, "live_consumer_evidence"),
          consumerMarker,
          parsePositiveInt(consumerCount, id, "consumer_marker_count"),
          effect,
          targetAuthority,
          status
        )
      }
    }

  private def parsePositiveInt(value: String, id: String, field: String): Int =
    Try(value.toInt).filter(_ > 0).getOrElse(throw new IllegalArgumentException(s"Invalid $field for $id: $value"))

  private def parseEvidence(value: String, id: String, field: String): Evidence = {
    val separator = value.lastIndexOf(':')
    require(separator > 0 && separator < value.length - 1, s"Invalid $field for $id: $value")
    val line = Try(value.substring(separator + 1).toInt)
      .filter(_ > 0)
      .getOrElse(throw new IllegalArgumentException(s"Invalid $field line for $id: $value"))
    Evidence(value.substring(0, separator), line)
  }

  private def productionScalaSources: IO[List[Source]] =
    repositoryRootF.map { root =>
      val stream = Files.walk(root.resolve("modules"))
      try
        stream
          .iterator()
          .asScala
          .filter(Files.isRegularFile(_))
          .filter(_.getFileName.toString.endsWith(".scala"))
          .filter(_.toString.replace('\\', '/').contains("/src/main/scala/"))
          .map { path =>
            Source(root.relativize(path).toString.replace('\\', '/'), read(path))
          }
          .toList
      finally stream.close()
    }

  private def productionGoSources: IO[List[Source]] =
    repositoryRootF.map { root =>
      val stream = Files.walk(root.resolve("p2p"))
      try
        stream
          .iterator()
          .asScala
          .filter(Files.isRegularFile(_))
          .filter(path => path.getFileName.toString.endsWith(".go") && !path.getFileName.toString.endsWith("_test.go"))
          .map { path =>
            Source(root.relativize(path).toString.replace('\\', '/'), read(path))
          }
          .toList
      finally stream.close()
    }

  private def repositoryRootF: IO[Path] =
    IO.blocking(repositoryRoot(Paths.get(sys.props("user.dir")).toAbsolutePath.normalize()))

  private def repositoryRoot(start: Path): Path =
    Iterator
      .iterate(Option(start))(_.flatMap(path => Option(path.getParent)))
      .takeWhile(_.nonEmpty)
      .flatten
      .find(path => Files.isRegularFile(path.resolve("build.sbt")) && Files.isDirectory(path.resolve("modules")))
      .getOrElse(throw new IllegalStateException(s"Cannot locate repository root from $start"))

  private def read(path: Path): String =
    new String(Files.readAllBytes(path), StandardCharsets.UTF_8)
}
