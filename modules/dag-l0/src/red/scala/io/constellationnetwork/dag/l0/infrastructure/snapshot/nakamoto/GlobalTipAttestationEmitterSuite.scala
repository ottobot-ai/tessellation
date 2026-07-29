package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import cats.effect.IO

import scala.jdk.CollectionConverters._
import scala.util.matching.Regex

import shapeless.test.illTyped
import weaver.SimpleIOSuite

/** RED source/API boundary for replay-gated GL0 optimistic authority.
  *
  * These checks intentionally name the two ratified authorities. They do not activate optimistic finality and they do not alter GL0's
  * execution responsibility: complete GL0 recreation still includes native GL1/DAG-token transitions. The suite should remain red until
  * the opaque receipt and validated-store APIs replace the publicly forgeable `Valid` plus raw `store` boundary.
  */
object GlobalTipAttestationEmitterSuite extends SimpleIOSuite {

  private val authenticatedDeclaration: Regex =
    "(?m)^\\s*sealed\\s+(?:trait|abstract\\s+class)\\s+AuthenticatedExecutedGlobalSnapshot\\b".r
  private val preferredDeclaration: Regex =
    "(?m)^\\s*sealed\\s+(?:trait|abstract\\s+class)\\s+PreferredExecutedTip\\b".r
  private val publiclyConcreteCapability: Regex =
    "(?m)^\\s*(?:final\\s+)?case\\s+class\\s+(?:AuthenticatedExecutedGlobalSnapshot|PreferredExecutedTip)\\b".r
  private val publicValidResult: Regex =
    "(?m)^\\s*(?:final\\s+)?case\\s+class\\s+Valid\\s*\\(".r
  private val rawStoreDefinition: Regex =
    "(?s)def\\s+store\\s*\\(\\s*signedSnapshot:\\s*Signed\\[GlobalIncrementalSnapshot\\]".r
  private val validatedStoreDefinition: Regex =
    "(?s)def\\s+storeValidated\\s*\\([^)]*AuthenticatedExecutedGlobalSnapshot".r
  private val recoverySeedDefinition: Regex =
    "(?s)def\\s+seedUnattestableForRecovery\\s*\\([^)]*Signed\\[GlobalIncrementalSnapshot\\]".r
  private val forbiddenCapabilityCodec: Regex =
    "(?s)(?:Encoder|Decoder|Codec|Kryo|Proto|protobuf)[^\\n]{0,160}(?:AuthenticatedExecutedGlobalSnapshot|PreferredExecutedTip)".r
  private val forbiddenAuthorityFactory: Regex =
    "(?m)def\\s+(?:from|unsafeFrom)(?:Accepted|Valid|Stored|BestTip|Hash|Root|SignatureCount)\\b".r

  test("RTA-RED-001: execution and preference capabilities are opaque, non-codec authorities") {
    readSources.map { sources =>
      val capabilitySources = sources.dagL0Main
      val publicCompanionFactories = List(
        "object AuthenticatedExecutedGlobalSnapshot",
        "object PreferredExecutedTip"
      ).count(capabilitySources.contains)

      expect.all(
        authenticatedDeclaration.findAllIn(capabilitySources).size == 1,
        preferredDeclaration.findAllIn(capabilitySources).size == 1,
        publiclyConcreteCapability.findFirstIn(capabilitySources).isEmpty,
        publicCompanionFactories == 0,
        forbiddenCapabilityCodec.findFirstIn(capabilitySources).isEmpty,
        !capabilitySources.contains("AuthenticatedExecutedGlobalSnapshot extends Product"),
        !capabilitySources.contains("PreferredExecutedTip extends Product")
      )
    }
  }

  test("RTA-RED-001: callers cannot construct, copy, apply, or decode either capability") {
    illTyped("""new AuthenticatedExecutedGlobalSnapshot {}""")
    illTyped("""AuthenticatedExecutedGlobalSnapshot.apply(null)""")
    illTyped("""null.asInstanceOf[AuthenticatedExecutedGlobalSnapshot].copy()""")
    illTyped("""implicitly[io.circe.Decoder[AuthenticatedExecutedGlobalSnapshot]]""")
    illTyped("""implicitly[scodec.Codec[AuthenticatedExecutedGlobalSnapshot]]""")
    illTyped("""new PreferredExecutedTip {}""")
    illTyped("""PreferredExecutedTip.apply(null)""")
    illTyped("""null.asInstanceOf[PreferredExecutedTip].copy()""")
    illTyped("""implicitly[io.circe.Decoder[PreferredExecutedTip]]""")
    illTyped("""implicitly[scodec.Codec[PreferredExecutedTip]]""")
    IO.pure(expect(true))
  }

  test("RTA-RED-003: receipt, storage, and selected-tip observations cannot fabricate execution authority") {
    readSources.map { sources =>
      expect.all(
        publicValidResult.findFirstIn(sources.validator).isEmpty,
        forbiddenAuthorityFactory.findFirstIn(sources.dagL0Main).isEmpty,
        rawStoreDefinition.findFirstIn(sources.chainStore).isEmpty,
        validatedStoreDefinition.findFirstIn(sources.chainStore).nonEmpty,
        !sources.chainStore.contains("Boolean => AuthenticatedExecutedGlobalSnapshot"),
        !sources.chainStore.contains("ValidationResult => AuthenticatedExecutedGlobalSnapshot"),
        !sources.chainStore.contains("StoredSnapshot => AuthenticatedExecutedGlobalSnapshot"),
        !sources.chainStore.contains("SelectedTip => AuthenticatedExecutedGlobalSnapshot")
      )
    }
  }

  test("RTA-RED-004: the complete receive authentication and replay chain precedes every validated store") {
    readSources.map { sources =>
      val receive = sliceBetween(
        sources.syncDaemon,
        "private def handleSnapshot[",
        "/** Process a VRF-validated snapshot AFTER"
      )
      val envelope = receive.indexOf("validateSnapshotEnvelope")
      val registry = receive.indexOf("resolveRegisteredOperatorKeys")
      val kes = receive.indexOf("KesGossipVerification.verifySnapshot")
      val replay = matchStartAfter("NakamotoSnapshotValidator\\s*\\.validate".r, receive, kes + 1)
      val receiptGate = receive.indexOf("commitReplayValidated(validationResult)")
      val validatedStore = receive.indexOf("chainStore.storeValidated")

      expect.all(
        envelope >= 0,
        registry > envelope,
        kes > registry,
        replay > kes,
        receiptGate > replay,
        validatedStore > receiptGate,
        matchStart("consensusFns\\s*\\.validateArtifact".r, sources.validator) >= 0,
        sources.validator.contains("AuthenticatedExecutedGlobalSnapshot"),
        !receive.substring(0, math.max(0, receiptGate)).contains("chainStore.storeValidated"),
        !receive.substring(0, math.max(0, receiptGate)).contains("PreferredExecutedTip")
      )
    }
  }

  test("RTA-RED-005/006: a restored head is unattestable until exact duplicate replay upgrades it") {
    readSources.map { sources =>
      val startupSeed = sliceBetween(
        sources.leaderLoop,
        "val seedChainStore: Stream[F, Unit]",
        "// Slot duration:"
      )
      val storeLower = sources.chainStore.toLowerCase(java.util.Locale.ROOT)

      expect.all(
        recoverySeedDefinition.findFirstIn(sources.chainStore).nonEmpty,
        startupSeed.contains("chainStore.seedUnattestableForRecovery"),
        !startupSeed.contains("chainStore.storeValidated"),
        !startupSeed.contains("PreferredExecutedTip"),
        validatedStoreDefinition.findFirstIn(sources.chainStore).nonEmpty,
        storeLower.contains("exact duplicate"),
        storeLower.contains("unattestable"),
        storeLower.contains("upgrade"),
        storeLower.contains("different hash") || storeLower.contains("hash mismatch")
      )
    }
  }

  test("RTA-RED-007: replay-valid alternate storage cannot carry or mint current-tip preference") {
    readSources.map { sources =>
      val storedAlternateParameters = caseClassParameters(sources.chainStore, "StoredAlternate")
      val duplicateParameters = caseClassParameters(sources.chainStore, "Duplicate")
      val becameSelectedParameters = caseClassParameters(sources.chainStore, "BecameSelected")
      val alternateReceiveBranch = sliceBetween(
        sources.syncDaemon,
        "case _: NakamotoChainStore.StoreOutcome.StoredAlternate",
        "case NakamotoChainStore.StoreOutcome.Rejected"
      )

      expect.all(
        storedAlternateParameters.nonEmpty,
        !storedAlternateParameters.exists(_.contains("PreferredExecutedTip")),
        duplicateParameters.exists(_.contains("Option[PreferredExecutedTip]")),
        becameSelectedParameters.exists(_.contains("PreferredExecutedTip")),
        !alternateReceiveBranch.contains("PreferredExecutedTip"),
        !alternateReceiveBranch.contains("emit("),
        !alternateReceiveBranch.contains(".sign"),
        !alternateReceiveBranch.contains("sign("),
        sources.chainStore.contains("PreferredExecutedTip"),
        sources.chainStore.contains("BecameSelected")
      )
    }
  }

  test("RTA-RED-017: buffered children re-enter handleSnapshot; parent arrival itself mints no authority") {
    readSources.map { sources =>
      val bufferHelper = sliceBetween(
        sources.syncDaemon,
        "private[nakamoto] def bufferPendingChild",
        "/** Run storage, canonical writes"
      )
      val productionDrain = sliceBetween(
        sources.syncDaemon,
        "private def drainPendingChildren[",
        "private def handleSnapshot["
      )

      expect.all(
        productionDrain.contains("handleSnapshot("),
        !productionDrain.contains("NakamotoSnapshotValidator.Valid("),
        !productionDrain.contains("AuthenticatedExecutedGlobalSnapshot("),
        !productionDrain.contains("chainStore.store"),
        !productionDrain.contains("PreferredExecutedTip"),
        !bufferHelper.contains("chainStore"),
        !bufferHelper.contains("AuthenticatedExecutedGlobalSnapshot"),
        !bufferHelper.contains("PreferredExecutedTip")
      )
    }
  }

  private final case class Sources(
    validator: String,
    chainStore: String,
    syncDaemon: String,
    leaderLoop: String,
    dagL0Main: String
  )

  private def readSources: IO[Sources] =
    IO.blocking {
      val root = repositoryRoot(Paths.get(sys.props("user.dir")).toAbsolutePath.normalize())
      def read(relative: String): String =
        new String(Files.readAllBytes(root.resolve(relative)), StandardCharsets.UTF_8)
      def readScalaTree(relative: String): String = {
        val paths = Files.walk(root.resolve(relative))
        try
          paths
            .iterator()
            .asScala
            .filter(path => Files.isRegularFile(path) && path.getFileName.toString.endsWith(".scala"))
            .map(path => new String(Files.readAllBytes(path), StandardCharsets.UTF_8))
            .mkString("\n")
        finally paths.close()
      }

      Sources(
        read("modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/NakamotoSnapshotValidator.scala"),
        read("modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/NakamotoChainStore.scala"),
        read("modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/NakamotoSyncDaemon.scala"),
        read("modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/SnapshotLeaderLoop.scala"),
        readScalaTree("modules/dag-l0/src/main/scala")
      )
    }

  private def caseClassParameters(source: String, name: String): Option[String] = {
    val declaration = s"final case class $name"
    val start = source.indexOf(declaration)
    if (start < 0) None
    else {
      val open = source.indexOf('(', start + declaration.length)
      if (open < 0) None
      else {
        var cursor = open
        var depth = 0
        var end = -1
        while (cursor < source.length && end < 0) {
          source.charAt(cursor) match {
            case '(' => depth += 1
            case ')' =>
              depth -= 1
              if (depth == 0) end = cursor
            case _ => ()
          }
          cursor += 1
        }
        Option.when(end >= 0)(source.substring(open + 1, end))
      }
    }
  }

  private def matchStart(regex: Regex, source: String): Int =
    regex.findFirstMatchIn(source).fold(-1)(_.start)

  private def matchStartAfter(regex: Regex, source: String, from: Int): Int =
    if (from < 0 || from >= source.length) -1
    else regex.findFirstMatchIn(source.substring(from)).fold(-1)(matched => from + matched.start)

  private def sliceBetween(source: String, startAnchor: String, endAnchor: String): String = {
    val start = source.indexOf(startAnchor)
    require(start >= 0, s"Missing source anchor: $startAnchor")
    val end = source.indexOf(endAnchor, start + startAnchor.length)
    require(end >= 0, s"Missing source anchor after '$startAnchor': $endAnchor")
    source.substring(start, end)
  }

  @annotation.tailrec
  private def repositoryRoot(candidate: Path): Path =
    if (Files.isDirectory(candidate.resolve("modules/dag-l0")) && Files.isRegularFile(candidate.resolve("build.sbt"))) candidate
    else
      Option(candidate.getParent) match {
        case Some(parent) => repositoryRoot(parent)
        case None         => throw new IllegalStateException(s"Unable to locate repository root from ${sys.props("user.dir")}")
      }
}
